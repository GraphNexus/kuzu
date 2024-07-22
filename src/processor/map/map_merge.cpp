#include "planner/operator/persistent/logical_merge.h"
#include "processor/operator/persistent/merge.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static FactorizedTableSchema getFactorizedTableSchema(const binder::expression_vector& keys) {
    auto tableSchema = FactorizedTableSchema();
    auto isUnFlat = false;
    auto groupID = 0u;
    for (auto& key : keys) {
        auto size = common::LogicalTypeUtils::getRowLayoutSize(key->dataType);
        tableSchema.appendColumn(ColumnSchema(isUnFlat, groupID, size));
    }
    tableSchema.appendColumn(ColumnSchema(isUnFlat, groupID, sizeof(bool)));
    tableSchema.appendColumn(ColumnSchema(isUnFlat, groupID, sizeof(common::hash_t)));
    return tableSchema;
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapMerge(planner::LogicalOperator* logicalOperator) {
    auto& logicalMerge = logicalOperator->constCast<LogicalMerge>();
    auto outSchema = logicalMerge.getSchema();
    auto inSchema = logicalMerge.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto existenceMarkPos = getDataPos(*logicalMerge.getExistenceMark(), *inSchema);
    std::vector<NodeInsertExecutor> nodeInsertExecutors;
    for (auto& info : logicalMerge.getInsertNodeInfos()) {
        nodeInsertExecutors.push_back(getNodeInsertExecutor(&info, *inSchema, *outSchema)->copy());
    }
    std::vector<RelInsertExecutor> relInsertExecutors;
    for (auto& info : logicalMerge.getInsertRelInfos()) {
        relInsertExecutors.push_back(getRelInsertExecutor(&info, *inSchema, *outSchema)->copy());
    }
    std::vector<std::unique_ptr<NodeSetExecutor>> onCreateNodeSetExecutors;
    for (auto& info : logicalMerge.getOnCreateSetNodeInfos()) {
        onCreateNodeSetExecutors.push_back(getNodeSetExecutor(info, *inSchema));
    }
    std::vector<std::unique_ptr<RelSetExecutor>> onCreateRelSetExecutors;
    for (auto& info : logicalMerge.getOnCreateSetRelInfos()) {
        onCreateRelSetExecutors.push_back(getRelSetExecutor(info, *inSchema));
    }
    std::vector<std::unique_ptr<NodeSetExecutor>> onMatchNodeSetExecutors;
    for (auto& info : logicalMerge.getOnMatchSetNodeInfos()) {
        onMatchNodeSetExecutors.push_back(getNodeSetExecutor(info, *inSchema));
    }
    std::vector<std::unique_ptr<RelSetExecutor>> onMatchRelSetExecutors;
    for (auto& info : logicalMerge.getOnMatchSetRelInfos()) {
        onMatchRelSetExecutors.push_back(getRelSetExecutor(info, *inSchema));
    }
    binder::expression_vector expressions;
    for (auto& info : logicalMerge.getInsertNodeInfos()) {
        for (auto& expr : info.columnExprs) {
            expressions.push_back(expr);
        }
    }
    for (auto& info : logicalMerge.getInsertRelInfos()) {
        for (auto& expr : info.columnExprs) {
            expressions.push_back(expr);
        }
    }
    std::vector<binder::expression_pair> onCreateOperation;
    for (auto& info : logicalMerge.getOnCreateSetRelInfos()) {
        onCreateOperation.push_back(info.setItem);
    }
    for (auto& info : logicalMerge.getOnCreateSetNodeInfos()) {
        onCreateOperation.push_back(info.setItem);
    }
    std::vector<binder::expression_pair> onMatchOperation;
    for (auto& info : logicalMerge.getOnMatchSetRelInfos()) {
        onMatchOperation.push_back(info.setItem);
    }
    for (auto& info : logicalMerge.getOnMatchSetNodeInfos()) {
        onMatchOperation.push_back(info.setItem);
    }
    auto printInfo =
        std::make_unique<MergePrintInfo>(expressions, onCreateOperation, onMatchOperation);
    MergeInfo mergeInfo{HashAggregateInfo{getDataPos(logicalMerge.getKeys(), *inSchema),
        std::vector<DataPos>{}, std::vector<DataPos>{},
        getFactorizedTableSchema(logicalMerge.getKeys()), HashTableType::MARK_HASH_TABLE}};
    return std::make_unique<Merge>(existenceMarkPos, std::move(nodeInsertExecutors),
        std::move(relInsertExecutors), std::move(onCreateNodeSetExecutors),
        std::move(onCreateRelSetExecutors), std::move(onMatchNodeSetExecutors),
        std::move(onMatchRelSetExecutors), std::move(mergeInfo), std::move(prevOperator),
        getOperatorID(), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
