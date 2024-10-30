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

struct FTSGDSBindData final : public function::GDSBindData {
    std::shared_ptr<binder::Expression> terms;
    // k: parameter controls the influence of term frequency saturation. It limits the effect of
    // additional occurrences of a term within a document.
    double_t k;
    // b: parameter controls the degree of length normalization by adjusting the influence of
    // document length.
    double_t b;
    uint64_t numDocs;
    double_t avgDocLen;

    FTSGDSBindData(std::shared_ptr<binder::Expression> terms,
        std::shared_ptr<binder::Expression> docs, double_t k, double_t b, uint64_t numDocs,
        double_t avgDocLen)
        : GDSBindData{std::move(docs)}, terms{std::move(terms)}, k{k}, b{b}, numDocs{numDocs},
          avgDocLen{avgDocLen} {}
    FTSGDSBindData(const FTSGDSBindData& other)
        : GDSBindData{other}, terms{other.terms}, k{other.k}, b{other.b}, numDocs{other.numDocs},
          avgDocLen{other.avgDocLen} {}

    bool hasNodeInput() const override { return true; }
    std::shared_ptr<binder::Expression> getNodeInput() const override { return terms; }

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<FTSGDSBindData>(*this);
    }
};

struct ScoreData {
    uint64_t df;
    uint64_t tf;

    ScoreData(uint64_t df, uint64_t tf) : df{df}, tf{tf} {}
};

struct ScoreInfo {
    nodeID_t termID;
    std::vector<ScoreData> scoreData;

    explicit ScoreInfo(nodeID_t termID) : termID{std::move(termID)} {}

    void addEdge(uint64_t df, uint64_t tf) { scoreData.emplace_back(df, tf); }
};

struct FTSEdgeCompute : public EdgeCompute {
    DoublePathLengthsFrontierPair* termsFrontier;
    common::node_id_map_t<ScoreInfo>* scores;
    common::node_id_map_t<uint64_t>* dfs;

    FTSEdgeCompute(DoublePathLengthsFrontierPair* termsFrontier,
        common::node_id_map_t<ScoreInfo>* scores, common::node_id_map_t<uint64_t>* dfs)
        : termsFrontier{termsFrontier}, scores{scores}, dfs{dfs} {}

    void edgeCompute(nodeID_t boundNodeID, std::span<const common::nodeID_t> nbrIDs,
        std::span<const relID_t>, SelectionVector& mask, bool /*isFwd*/,
        const ValueVector* edgeProp) override;

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<FTSEdgeCompute>(termsFrontier, scores, dfs);
    }
};

void FTSEdgeCompute::edgeCompute(nodeID_t boundNodeID, std::span<const common::nodeID_t> nbrIDs,
    std::span<const relID_t>, SelectionVector& mask, bool /*isFwd*/, const ValueVector* edgeProp) {
    KU_ASSERT(dfs->contains(boundNodeID));
    size_t activeCount = 0;
    mask.forEach([&](auto i) {
        auto docID = nbrIDs[i];
        auto df = dfs->at(boundNodeID);
        auto tf = edgeProp->getValue<uint64_t>(i);
        if (!scores->contains(docID)) {
            scores->emplace(docID, ScoreInfo{boundNodeID});
        }
        scores->at(docID).addEdge(df, tf);
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
    graph::Graph* graph, common::idx_t tfPropertyIdx) {
    auto frontierPair = ftsState.frontierPair.get();
    frontierPair->beginNewIteration();
    auto relTableIDInfos = graph->getRelTableIDInfos();
    auto& termsTableInfo = relTableIDInfos[0];
    frontierPair->beginFrontierComputeBetweenTables(termsTableInfo.fromNodeTableID,
        termsTableInfo.toNodeTableID);
    GDSUtils::scheduleFrontierTask(termsTableInfo.relTableID, graph, ExtendDirection::FWD, ftsState,
        executionContext, FTSAlgorithm::NUM_THREADS_FOR_EXECUTION, tfPropertyIdx);
}

class FTSOutputWriter {
public:
    FTSOutputWriter(storage::MemoryManager* mm, FTSOutput* ftsOutput,
        const FTSGDSBindData& bindData);

    void write(processor::FactorizedTable& scoreFT, nodeID_t docNodeID, uint64_t len);

    std::unique_ptr<FTSOutputWriter> copy();

private:
    FTSOutput* ftsOutput;
    common::ValueVector termsVector;
    common::ValueVector docsVector;
    common::ValueVector scoreVector;
    std::vector<common::ValueVector*> vectors;
    common::idx_t pos;
    storage::MemoryManager* mm;
    const FTSGDSBindData& bindData;
};

FTSOutputWriter::FTSOutputWriter(storage::MemoryManager* mm, FTSOutput* ftsOutput,
    const FTSGDSBindData& bindData)
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

void FTSOutputWriter::write(processor::FactorizedTable& scoreFT, nodeID_t docNodeID, uint64_t len) {
    bool hasScore = ftsOutput->scores.contains(docNodeID);
    termsVector.setNull(pos, !hasScore);
    docsVector.setNull(pos, !hasScore);
    scoreVector.setNull(pos, !hasScore);
    // See comments in FTSBindData for the meaning of k and b.
    auto k = bindData.k;
    auto b = bindData.b;
    if (hasScore) {
        auto scoreInfo = ftsOutput->scores.at(docNodeID);
        double score = 0;
        for (auto& scoreData : scoreInfo.scoreData) {
            auto numDocs = bindData.numDocs;
            auto avgDocLen = bindData.avgDocLen;
            auto df = scoreData.df;
            auto tf = scoreData.tf;
            score += log10((numDocs - df + 0.5) / (df + 0.5) + 1) *
                     ((tf * (k + 1) / (tf + k * (1 - b + b * (len / avgDocLen)))));
        }
        termsVector.setValue(pos, scoreInfo.termID);
        docsVector.setValue(pos, docNodeID);
        scoreVector.setValue(pos, score);
    }
    scoreFT.append(vectors);
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
        scoreFT = std::make_unique<processor::FactorizedTable>(sharedState->mm,
            sharedState->globalFT->getTableSchema()->copy());
        localScoreOutputWriter = sharedState->ftsOutputWriter->copy();
    }

    void vertexCompute(std::span<const nodeID_t> nodeIDs,
        std::span<const std::unique_ptr<ValueVector>> properties) override;

    void finalizeWorkerThread() override {
        std::unique_lock lck(sharedState->mtx);
        sharedState->globalFT->merge(*scoreFT);
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<FTSOutputWriterVC>(sharedState);
    }

private:
    FTSOutputWriterSharedState* sharedState;
    std::unique_ptr<processor::FactorizedTable> scoreFT;
    std::unique_ptr<FTSOutputWriter> localScoreOutputWriter;
};

void FTSOutputWriterVC::vertexCompute(std::span<const nodeID_t> nodeIDs,
    std::span<const std::unique_ptr<ValueVector>> properties) {
    for (auto i = 0u; i < nodeIDs.size(); i++) {
        localScoreOutputWriter->write(*scoreFT, nodeIDs[i], properties[0]->getValue<uint64_t>(i));
    }
}

void runVertexComputeIteration(processor::ExecutionContext* executionContext, graph::Graph* graph,
    VertexCompute& vc) {
    auto maxThreads = executionContext->clientContext->getCurrentSetting(main::ThreadsSetting::name)
                          .getValue<uint64_t>();
    auto sharedState = std::make_shared<VertexComputeTaskSharedState>(maxThreads, graph);
    auto docTableID = graph->getNodeTableIDs()[1];
    auto info = VertexComputeTaskInfo(vc, {FTSAlgorithm::DOC_LEN_PROP_NAME});
    GDSUtils::runVertexComputeOnTable(docTableID, graph, sharedState, info, *executionContext);
}

FTSState::FTSState(std::unique_ptr<function::FrontierPair> frontierPair,
    std::unique_ptr<function::EdgeCompute> edgeCompute, common::table_id_t termTableID)
    : function::GDSComputeState{std::move(frontierPair), std::move(edgeCompute)} {
    this->frontierPair->getNextFrontierUnsafe().ptrCast<PathLengths>()->fixNextFrontierNodeTable(
        termTableID);
}

void FTSState::setNodeActive(common::nodeID_t termID) const {
    frontierPair->getNextFrontierUnsafe().ptrCast<PathLengths>()->setActive(termID);
}

void FTSAlgorithm::exec(processor::ExecutionContext* executionContext) {
    auto termTableID = sharedState->graph->getNodeTableIDs()[0];
    if (!sharedState->getInputNodeMaskMap()->containsTableID(termTableID)) {
        return;
    }
    auto terms = sharedState->getInputNodeMaskMap();
    auto output = std::make_unique<FTSOutput>();
    auto termMask = terms->getOffsetMask(termTableID);

    // Do edge compute to extend terms -> docs and save the term frequency and document frequency
    // for each term-doc pair. The reason why we store the term frequency and document frequency
    // is that: we need the `len` property from the docs table which is only available during the
    // vertex compute.
    auto frontierPair = std::make_unique<DoublePathLengthsFrontierPair>(
        sharedState->graph->getNodeTableIDAndNumNodes(),
        executionContext->clientContext->getMaxNumThreadForExec(),
        executionContext->clientContext->getMemoryManager());
    auto edgeCompute = std::make_unique<FTSEdgeCompute>(frontierPair.get(), &output->scores,
        sharedState->nodeProp);
    FTSState ftsState = FTSState{std::move(frontierPair), std::move(edgeCompute), termTableID};
    auto termNodeID = nodeID_t{INVALID_OFFSET, termTableID};
    for (auto offset = 0u; offset < sharedState->graph->getNumNodes(termTableID); ++offset) {
        if (!termMask->isMasked(offset)) {
            continue;
        }
        termNodeID.offset = offset;
        ftsState.setNodeActive(termNodeID);
    }
    runFrontiersOnce(executionContext, ftsState, sharedState->graph.get(),
        executionContext->clientContext->getCatalog()
            ->getTableCatalogEntry(executionContext->clientContext->getTx(),
                sharedState->graph->getRelTableIDs()[0])
            ->getPropertyIdx(FTSAlgorithm::TERM_FREQUENCY_PROP_NAME));

    // Do vertex compute to calculate the score for doc with the length property.
    FTSOutputWriter outputWriter{executionContext->clientContext->getMemoryManager(), output.get(),
        *bindData->ptrCast<FTSGDSBindData>()};
    auto ftsOutputWriterSharedState = std::make_unique<FTSOutputWriterSharedState>(
        executionContext->clientContext->getMemoryManager(), sharedState->fTable.get(),
        &outputWriter);
    auto writerVC = std::make_unique<FTSOutputWriterVC>(ftsOutputWriterSharedState.get());
    runVertexComputeIteration(executionContext, sharedState->graph.get(), *writerVC);
}

static std::shared_ptr<Expression> getScoreColumn(Binder* binder) {
    return binder->createVariable(FTSAlgorithm::SCORE_PROP_NAME, LogicalType::DOUBLE());
}

binder::expression_vector FTSAlgorithm::getResultColumns(binder::Binder* binder) const {
    expression_vector columns;
    auto& termNode = bindData->getNodeInput()->constCast<NodeExpression>();
    columns.push_back(termNode.getInternalID());
    auto& docNode = bindData->getNodeOutput()->constCast<NodeExpression>();
    columns.push_back(docNode.getInternalID());
    columns.push_back(getScoreColumn(binder));
    return columns;
}

void FTSAlgorithm::bind(const binder::expression_vector& params, binder::Binder* binder,
    graph::GraphEntry& graphEntry) {
    KU_ASSERT(params.size() == 6);
    auto termNode = params[1];
    auto k = ExpressionUtil::getLiteralValue<double>(*params[2]);
    auto b = ExpressionUtil::getLiteralValue<double>(*params[3]);
    auto numDocs = ExpressionUtil::getLiteralValue<uint64_t>(*params[4]);
    auto avgDocLen = ExpressionUtil::getLiteralValue<double>(*params[5]);
    auto nodeOutput = bindNodeOutput(binder, graphEntry);
    bindData = std::make_unique<FTSGDSBindData>(termNode, nodeOutput, k, b, numDocs, avgDocLen);
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
