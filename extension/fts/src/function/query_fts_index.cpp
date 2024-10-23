#include "function/query_fts_index.h"

#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "fts_extension.h"
#include "function/table/bind_input.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::function;

struct QueryFTSBindData final : public CallTableFuncBindData {
    std::string tableName;
    std::string query;

    QueryFTSBindData(std::string tableName, std::string query, std::vector<LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          tableName{std::move(tableName)}, query{std::move(query)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<QueryFTSBindData>(tableName, query, LogicalType::copy(columnTypes),
            columnNames, maxOffset);
    }
};

struct QueryFTSLocalState : public TableFuncLocalState {
    std::unique_ptr<QueryResult> result = nullptr;
    uint64_t numRowsOutput = 0;
};

static LogicalType bindNodeType(std::string tableName, ClientContext* context) {
    if (!context->getCatalog()->containsTable(context->getTx(), tableName)) {
        throw BinderException{common::stringFormat("Table {} does not exist.", tableName)};
    }
    std::vector<StructField> nodeFields;
    nodeFields.emplace_back(InternalKeyword::ID, LogicalType::INTERNAL_ID());
    nodeFields.emplace_back(InternalKeyword::LABEL, LogicalType::STRING());
    auto tableEntry = context->getCatalog()->getTableCatalogEntry(context->getTx(), tableName);
    for (auto& property : tableEntry->getProperties()) {
        nodeFields.emplace_back(property.getName(), property.getType().copy());
    }
    return LogicalType::NODE(std::make_unique<StructTypeInfo>(std::move(nodeFields)));
}

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    ScanTableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    auto tableName = input->inputs[0].toString();
    columnTypes.push_back(bindNodeType(tableName, context));
    columnNames.push_back("node");
    columnTypes.push_back(LogicalType::DOUBLE());
    columnNames.push_back("score");
    auto query = input->inputs[2].toString();
    return std::make_unique<QueryFTSBindData>(tableName, query, std::move(columnTypes),
        std::move(columnNames), 1);
}

static common::offset_t tableFunc(TableFuncInput& data, TableFuncOutput& output) {
    auto localState = data.localState->ptrCast<QueryFTSLocalState>();
    if (localState->result == nullptr) {
        auto bindData = data.bindData->constPtrCast<QueryFTSBindData>();
        auto sharedState = data.sharedState->ptrCast<CallFuncSharedState>();
        if (!sharedState->getMorsel().hasMoreToOutput()) {
            return 0;
        }
        auto tableName = bindData->tableName;
        auto query =
            common::stringFormat("MATCH (a:{}_stats) RETURN a.num_docs, a.avg_dl", tableName);
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
                                     "WHERE _node.offset = offset(id(p)) "
                                     "RETURN p, score",
            tableName, tableName, tableName, bindData->query, tableName, numDocs, avgDL, tableName);
        localState->result = data.context->runQuery(query);
    }
    auto resultSize = localState->result->getNumTuples();
    if (localState->numRowsOutput >= resultSize) {
        return 0;
    }
    auto resultTable = localState->result->getTable();
    resultTable->scan(output.vectors, localState->numRowsOutput, 1);
    localState->numRowsOutput++;
    return 1;
}

std::unique_ptr<TableFuncLocalState> initLocalState(kuzu::function::TableFunctionInitInput& input,
    kuzu::function::TableFuncSharedState*, storage::MemoryManager*) {
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
