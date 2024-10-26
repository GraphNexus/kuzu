#include "function/drop_fts_index.h"

#include "binder/ddl/bound_alter_info.h"
#include "binder/expression/expression_util.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "common/types/value/nested.h"
#include "fts_extension.h"
#include "function/fts_utils.h"
#include "function/table/bind_input.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::function;

struct DropFTSBindData final : public StandaloneTableFuncBindData {
    std::string tableName;
    std::string indexName;

    DropFTSBindData(std::string tableName, std::string indexName)
        : StandaloneTableFuncBindData{}, tableName{std::move(tableName)},
          indexName{std::move(indexName)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<DropFTSBindData>(tableName, indexName);
    }
};

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    ScanTableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.push_back("");
    columnTypes.push_back(LogicalType::STRING());
    auto indexName = input->inputs[1].toString();
    auto& tableEntry =
        FTSUtils::bindTable(input->inputs[0], context, indexName, FTSUtils::IndexOperation::DROP);
    FTSUtils::validateIndexExistence(tableEntry, indexName);
    return std::make_unique<DropFTSBindData>(tableEntry.getName(), indexName);
}

std::string dropFTSIndexQuery(ClientContext& context, const TableFuncBindData& bindData) {
    auto createFTSBindData = bindData.constPtrCast<DropFTSBindData>();
    auto tableName = createFTSBindData->tableName;
    auto indexName = createFTSBindData->indexName;
    binder::BoundAlterInfo boundAlterInfo{common::AlterType::DROP_INDEX, tableName,
        std::make_unique<binder::BoundExtraIndexInfo>(indexName)};
    context.getTransactionContext()->commit();
    context.getTransactionContext()->beginAutoTransaction(false /* readOnly */);
    context.getCatalog()->alterTableEntry(context.getTx(), std::move(boundAlterInfo));
    context.getTransactionContext()->commit();
    context.getTransactionContext()->beginAutoTransaction(true /* readOnly */);
    auto tablePrefix = common::stringFormat("{}_{}", tableName, indexName);
    std::string query = common::stringFormat("DROP TABLE {}_stopwords;", tablePrefix);
    query += common::stringFormat("DROP TABLE {}_terms_in_doc;", tablePrefix);
    query += common::stringFormat("DROP TABLE {}_terms;", tablePrefix);
    query += common::stringFormat("DROP TABLE {}_docs;", tablePrefix);
    query += common::stringFormat("DROP TABLE {}_dict;", tablePrefix);
    query += common::stringFormat("DROP TABLE {}_stats;", tablePrefix);
    return query;
}

static common::offset_t tableFunc(TableFuncInput& /*data*/, TableFuncOutput& /*output*/) {
    KU_UNREACHABLE;
}

function_set DropFTSFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, tableFunc, bindFunc, initSharedState,
        initEmptyLocalState,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING});
    func->rewriteFunc = dropFTSIndexQuery;
    func->canParallelFunc = []() { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace fts_extension
} // namespace kuzu
