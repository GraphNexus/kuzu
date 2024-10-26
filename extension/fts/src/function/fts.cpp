#include "function/fts.h"

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "catalog/catalog.h"
#include "common/exception/runtime.h"
#include "common/task_system/task_scheduler.h"
#include "common/types/internal_id_util.h"
#include "function/gds/gds.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_task.h"
#include "function/gds/gds_utils.h"
#include "main/settings.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace fts_extension {

using namespace function;

struct ScoreData {
    uint64_t df;
    uint64_t tf;

    ScoreData(uint64_t df, uint64_t tf) : df{df}, tf{tf} {}
};

struct ScoreInfo {
    nodeID_t srcNode;
    std::vector<ScoreData> scoreData;

    explicit ScoreInfo(nodeID_t srcNode) : srcNode{std::move(srcNode)} {}

    void addEdge(uint64_t df, uint64_t tf) { scoreData.push_back(ScoreData{df, tf}); }
};

struct FTSEdgeCompute : public EdgeCompute {
    DoublePathLengthsFrontierPair* frontierPair;
    common::node_id_map_t<ScoreInfo>* scores;
    std::mutex mtx;
    common::node_id_map_t<uint64_t>* dfs;
    FTSEdgeCompute(DoublePathLengthsFrontierPair* frontierPair,
        common::node_id_map_t<ScoreInfo>* scores, common::node_id_map_t<uint64_t>* dfs)
        : frontierPair{frontierPair}, scores{scores}, dfs{dfs} {}

    void edgeCompute(nodeID_t boundNodeID, std::span<const common::nodeID_t> nbrIDs,
        std::span<const relID_t>, SelectionVector& mask, bool /*isFwd*/,
        const ValueVector* edgeProp) override;

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<FTSEdgeCompute>(frontierPair, scores, dfs);
    }
};

void FTSEdgeCompute::edgeCompute(nodeID_t boundNodeID, std::span<const common::nodeID_t> nbrIDs,
    std::span<const relID_t>, SelectionVector& mask, bool /*isFwd*/, const ValueVector* edgeProp) {
    KU_ASSERT(dfs->contains(boundNodeID));
    size_t activeCount = 0;
    std::lock_guard<std::mutex> guard{mtx};
    mask.forEach([&](auto i) {
        auto nbrNodeID = nbrIDs[i];
        auto df = dfs->at(boundNodeID);
        auto tf = edgeProp->getValue<uint64_t>(i);
        if (!scores->contains(nbrNodeID)) {
            scores->emplace(nbrNodeID, ScoreInfo{boundNodeID});
        }
        scores->at(nbrNodeID).addEdge(df, tf);
        mask.getMutableBuffer()[activeCount++] = i;
    });
    mask.setToFiltered(activeCount);
}

struct FTSOutput {
    common::node_id_map_t<ScoreInfo> scores;

    FTSOutput() = default;
    virtual ~FTSOutput() = default;
};

void runFrontiersOnce(processor::ExecutionContext* executionContext, FTSState& ftsState,
    graph::Graph* graph, common::idx_t edgePropertyIndex) {
    auto frontierPair = ftsState.frontierPair.get();
    frontierPair->beginNewIteration();
    auto relTableIDInfos = graph->getRelTableIDInfos();
    auto& relTableIDInfo = relTableIDInfos[0];
    frontierPair->beginFrontierComputeBetweenTables(relTableIDInfo.fromNodeTableID,
        relTableIDInfo.toNodeTableID);
    auto sharedState = std::make_shared<FrontierTaskSharedState>(*frontierPair);
    auto clientContext = executionContext->clientContext;
    auto info = FrontierTaskInfo(relTableIDInfo.relTableID, graph, common::ExtendDirection::FWD,
        *ftsState.edgeCompute, edgePropertyIndex);
    auto maxThreads =
        clientContext->getCurrentSetting(main::ThreadsSetting::name).getValue<uint64_t>();
    auto task = std::make_shared<FrontierTask>(maxThreads, info, sharedState);
    clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(task, executionContext,
        true /* launchNewWorkerThread */);
}

class FTSOutputWriter {
public:
    FTSOutputWriter(storage::MemoryManager* mm, FTSOutput* ftsOutput, const FTSBindData& bindData);

    void write(processor::FactorizedTable& fTable, nodeID_t docNodeID, uint64_t len);

    std::unique_ptr<FTSOutputWriter> copy();

private:
    FTSOutput* ftsOutput;
    common::ValueVector termsVector;
    common::ValueVector docsVector;
    common::ValueVector scoreVector;
    std::vector<common::ValueVector*> vectors;
    common::idx_t pos;
    storage::MemoryManager* mm;
    const FTSBindData& bindData;
};

FTSOutputWriter::FTSOutputWriter(storage::MemoryManager* mm, FTSOutput* ftsOutput,
    const FTSBindData& bindData)
    : ftsOutput{std::move(ftsOutput)}, termsVector{LogicalType::INTERNAL_ID(), mm},
      docsVector{LogicalType::INTERNAL_ID(), mm}, scoreVector{LogicalType::UINT64(), mm}, mm{mm},
      bindData{bindData} {
    auto state = DataChunkState::getSingleValueDataChunkState();
    pos = state->getSelVector()[0];
    termsVector.setState(state);
    docsVector.setState(state);
    scoreVector.setState(state);
    vectors.push_back(&termsVector);
    vectors.push_back(&docsVector);
    vectors.push_back(&scoreVector);
}

void FTSOutputWriter::write(processor::FactorizedTable& fTable, nodeID_t docNodeID, uint64_t len) {
    bool hasScore = ftsOutput->scores.contains(docNodeID);
    termsVector.setNull(pos, !hasScore);
    docsVector.setNull(pos, !hasScore);
    scoreVector.setNull(pos, !hasScore);
    if (hasScore) {
        auto scoreInfo = ftsOutput->scores.at(docNodeID);
        double score = 0;
        for (auto& scoreData : scoreInfo.scoreData) {
            auto numDocs = bindData.numDocs;
            auto avgDocLen = bindData.avgDocLen;
            auto df = scoreData.df;
            auto tf = scoreData.tf;
            auto k = bindData.k;
            auto b = bindData.b;
            score += log10((numDocs - df + 0.5) / (df + 0.5) + 1) *
                     ((tf * (k + 1) / (tf + k * (1 - b + b * (len / avgDocLen)))));
        }
        termsVector.setValue(pos, scoreInfo.srcNode);
        docsVector.setValue(pos, docNodeID);
        scoreVector.setValue(pos, score);
    }
    fTable.append(vectors);
}

std::unique_ptr<FTSOutputWriter> FTSOutputWriter::copy() {
    return std::make_unique<FTSOutputWriter>(mm, ftsOutput, bindData);
}

class FTSOutputWriterSharedState {
public:
    FTSOutputWriterSharedState(storage::MemoryManager* mm, processor::FactorizedTable* globalFT,
        FTSOutputWriter* ftsOutputWriter)
        : mm{mm}, globalFT{globalFT}, ftsOutputWriter{ftsOutputWriter} {}

    std::mutex mtx;
    storage::MemoryManager* mm;
    processor::FactorizedTable* globalFT;
    FTSOutputWriter* ftsOutputWriter;
};

class FTSOutputWriterVC : public VertexCompute {
public:
    explicit FTSOutputWriterVC(FTSOutputWriterSharedState* sharedState) : sharedState{sharedState} {
        localFT = std::make_unique<processor::FactorizedTable>(sharedState->mm,
            sharedState->globalFT->getTableSchema()->copy());
        localFTSOutputWriter = sharedState->ftsOutputWriter->copy();
    }

    void beginOnTable(table_id_t /*tableID*/) override {}

    void vertexCompute(std::span<const nodeID_t> nodeIDs,
        std::span<const std::unique_ptr<ValueVector>> properties) override {
        for (auto i = 0u; i < nodeIDs.size(); i++) {
            localFTSOutputWriter->write(*localFT, nodeIDs[i], properties[0]->getValue<uint64_t>(i));
        }
    }

    void finalizeWorkerThread() override {
        std::unique_lock lck(sharedState->mtx);
        sharedState->globalFT->merge(*localFT);
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<FTSOutputWriterVC>(sharedState);
    }

private:
    FTSOutputWriterSharedState* sharedState;
    std::unique_ptr<processor::FactorizedTable> localFT;
    std::unique_ptr<FTSOutputWriter> localFTSOutputWriter;
};

void runVertexComputeIteration(processor::ExecutionContext* executionContext, graph::Graph* graph,
    VertexCompute& vc) {
    auto sharedState = std::make_shared<VertexComputeTaskSharedState>(
        executionContext->clientContext->getCurrentSetting(main::ThreadsSetting::name)
            .getValue<uint64_t>(),
        graph);
    auto tableID = graph->getNodeTableIDs()[1];
    sharedState->morselDispatcher.init(tableID, graph->getNumNodes(tableID));
    auto info = VertexComputeTaskInfo(vc, std::vector<std::string>{"len"});
    auto task = std::make_shared<VertexComputeTask>(
        executionContext->clientContext->getCurrentSetting(main::ThreadsSetting::name)
            .getValue<uint64_t>(),
        info, sharedState);
    executionContext->clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(task,
        executionContext, true /* launchNewWorkerThread */);
}

void FTSAlgorithm::exec(processor::ExecutionContext* executionContext) {
    auto termTableID = sharedState->graph->getNodeTableIDs()[0];
    if (!sharedState->getInputNodeMaskMap()->containsTableID(termTableID)) {
        return;
    }
    auto termMask = sharedState->getInputNodeMaskMap();
    auto output = std::make_unique<FTSOutput>();
    auto mask = termMask->getOffsetMask(termTableID);
    for (auto offset = 0u; offset < sharedState->graph->getNumNodes(termTableID); ++offset) {
        if (!mask->isMasked(offset)) {
            continue;
        }
        auto termNodeID = nodeID_t{offset, termTableID};
        auto frontierPair = std::make_unique<DoublePathLengthsFrontierPair>(
            sharedState->graph->getNodeTableIDAndNumNodes(),
            executionContext->clientContext->getMaxNumThreadForExec(),
            executionContext->clientContext->getMemoryManager());
        auto edgeCompute = std::make_unique<FTSEdgeCompute>(frontierPair.get(), &output->scores,
            sharedState->nodeProp);
        FTSState ftsState = FTSState{std::move(frontierPair), std::move(edgeCompute)};
        ftsState.initFTSFromSource(termNodeID);

        runFrontiersOnce(executionContext, ftsState, sharedState->graph.get(),
            executionContext->clientContext->getCatalog()
                ->getTableCatalogEntry(executionContext->clientContext->getTx(),
                    sharedState->graph->getRelTableIDs()[0])
                ->getPropertyIdx(FTSAlgorithm::TERM_FREQUENCY_PROP_NAME));
    }
    FTSOutputWriter outputWriter{executionContext->clientContext->getMemoryManager(), output.get(),
        *bindData->ptrCast<FTSBindData>()};
    auto ftsOutputWriterSharedState = std::make_unique<FTSOutputWriterSharedState>(
        executionContext->clientContext->getMemoryManager(), sharedState->fTable.get(),
        &outputWriter);
    auto writerVC = std::make_unique<FTSOutputWriterVC>(ftsOutputWriterSharedState.get());
    runVertexComputeIteration(executionContext, sharedState->graph.get(), *writerVC);
}

static std::shared_ptr<Expression> getScoreColumn(Binder* binder) {
    return binder->createVariable(FTSAlgorithm::SCORE_COLUMN_NAME, LogicalType::DOUBLE());
}

binder::expression_vector FTSAlgorithm::getResultColumns(binder::Binder* binder) const {
    expression_vector columns;
    auto& inputNode = bindData->getNodeInput()->constCast<NodeExpression>();
    columns.push_back(inputNode.getInternalID());
    auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
    columns.push_back(outputNode.getInternalID());
    columns.push_back(getScoreColumn(binder));
    return columns;
}

void FTSAlgorithm::bind(const binder::expression_vector& params, binder::Binder* binder,
    graph::GraphEntry& graphEntry) {
    KU_ASSERT(params.size() == 6);
    auto nodeInput = params[1];
    auto k = ExpressionUtil::getLiteralValue<double>(*params[2]);
    auto b = ExpressionUtil::getLiteralValue<double>(*params[3]);
    auto numDocs = ExpressionUtil::getLiteralValue<uint64_t>(*params[4]);
    auto avgDocLen = ExpressionUtil::getLiteralValue<double>(*params[5]);
    auto nodeOutput = bindNodeOutput(binder, graphEntry);
    bindData = std::make_unique<FTSBindData>(nodeInput, nodeOutput, k, b, numDocs, avgDocLen);
}

function::function_set FTSFunction::getFunctionSet() {
    function_set result;
    auto algo = std::make_unique<FTSAlgorithm>();
    result.push_back(
        std::make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo)));
    return result;
}

} // namespace fts_extension
} // namespace kuzu
