#include "function/query_fts_index.h"

#include "binder/expression/expression_util.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "fts_extension.h"
#include "function/fts_utils.h"
#include "function/table/bind_input.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::function;

struct QueryFTSBindData final : public CallTableFuncBindData {
    std::string tableName;
    std::string indexName;
    std::string query;

    QueryFTSBindData(std::string tableName, std::string indexName, std::string query,
        std::vector<LogicalType> returnTypes, std::vector<std::string> returnColumnNames,
        offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          tableName{std::move(tableName)}, indexName{std::move(indexName)},
          query{std::move(query)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<QueryFTSBindData>(*this);
    }
};

struct QueryFTSLocalState : public TableFuncLocalState {
    std::unique_ptr<QueryResult> result = nullptr;
    uint64_t numRowsOutput = 0;
};

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    ScanTableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    auto& tableEntry = FTSUtils::bindTable(input->inputs[0], context);
    auto indexName = input->inputs[1].toString();
    auto query = input->inputs[2].toString();
    FTSUtils::validateIndexExistence(tableEntry, indexName);
    columnTypes.push_back(common::StructType::getNodeType(tableEntry));
    columnNames.push_back("node");
    columnTypes.push_back(LogicalType::DOUBLE());
    columnNames.push_back("score");
    return std::make_unique<QueryFTSBindData>(tableEntry.getName(), std::move(indexName),
        std::move(query), std::move(columnTypes), std::move(columnNames), 1);
}

static common::offset_t tableFunc(TableFuncInput& data, TableFuncOutput& output) {
    // TODO(Xiyang/Ziyi): Currently we don't have a dedicated planner for queryFTS, so
    //  we need a wrapper call function to CALL the actual GDS function.
    auto localState = data.localState->ptrCast<QueryFTSLocalState>();
    if (localState->result == nullptr) {
        auto bindData = data.bindData->constPtrCast<QueryFTSBindData>();
        auto tablePrefix = bindData->tableName + "_" + bindData->indexName;
        auto query =
            common::stringFormat("MATCH (a:{}_stats) RETURN a.num_docs, a.avg_dl", tablePrefix);
        auto result = data.context->runQuery(query);
        auto tuple = result->getNext();
        auto numDocs = tuple->getValue(0)->getValue<uint64_t>();
        auto avgDL = tuple->getValue(1)->getValue<double_t>();
        query = common::stringFormat("PROJECT GRAPH PK ({}_dict, {}_docs, {}_terms) "
                                     "UNWIND tokenize('{}') AS tk "
                                     "WITH collect(stem(tk, 'porter')) AS tokens "
                                     "MATCH (a:{}_dict) "
                                     "WHERE list_contains(tokens, a.term) "
                                     "CALL FTS(PK, a, 1.2, 0.75, cast({} as UINT64), {}) "
                                     "MATCH (p:{}) "
                                     "WHERE _node.docID = offset(id(p)) "
                                     "RETURN p, score",
            tablePrefix, tablePrefix, tablePrefix, bindData->query, tablePrefix, numDocs, avgDL,
            bindData->tableName);
        localState->result = data.context->runQuery(query);
    }
    if (localState->numRowsOutput >= localState->result->getNumTuples()) {
        return 0;
    }
    auto resultTable = localState->result->getTable();
    resultTable->scan(output.vectors, localState->numRowsOutput, 1 /* numRowsToScan */);
    localState->numRowsOutput++;
    return 1;
}

std::unique_ptr<TableFuncLocalState> initLocalState(
    kuzu::function::TableFunctionInitInput& /*input*/, kuzu::function::TableFuncSharedState*,
    storage::MemoryManager*) {
    return std::make_unique<QueryFTSLocalState>();
}

function_set QueryFTSFunction::getFunctionSet() {
    function_set functionSet;
    auto func =
        std::make_unique<TableFunction>(name, tableFunc, bindFunc, initSharedState, initLocalState,
            std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
                LogicalTypeID::STRING});
    func->canParallelFunc = []() { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace fts_extension
} // namespace kuzu
