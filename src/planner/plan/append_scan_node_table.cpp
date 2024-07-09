#include "binder/expression/property_expression.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "planner/operator/logical_hash_join.h"
#include "planner/planner.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_reference_catalog_entry.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::catalog;

namespace kuzu {
namespace planner {

static expression_vector removeInternalIDProperty(const expression_vector& expressions) {
    expression_vector result;
    for (auto expr : expressions) {
        if (expr->constCast<PropertyExpression>().isInternalID()) {
            continue;
        }
        result.push_back(expr);
    }
    return result;
}

void Planner::appendScanNodeTable(const Expression& expr, const expression_vector& properties, LogicalPlan& plan) {
    auto& node = expr.constCast<NodeExpression>();
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTx();
    if (node.hasExternalEntry()) {
        auto nodeTableID = node.getSingleTableID();
        auto referenceEntry = catalog->getTableCatalogEntry(transaction, nodeTableID)->ptrCast<NodeTableReferenceCatalogEntry>();
        // Scan from physical storage the primary key column
        auto buildPlan = LogicalPlan();
        auto physicalEntry = referenceEntry->getPhysicalEntry()->ptrCast<NodeTableCatalogEntry>();
        auto pkIdx = referenceEntry->getPrimaryKeyIdx();
        auto pkExpr = node.getPropertyExpression(pkIdx);
        std::vector<LogicalNodeTableScanInfo> tableScanInfos;
        tableScanInfos.emplace_back(physicalEntry->getTableID(), std::vector<column_id_t>{physicalEntry->getPrimaryKey()->getColumnID()});
        appendScanNodeTable(node.getInternalID(), {pkExpr}, tableScanInfos, buildPlan);
        // Scan from external table
        auto externalEntry = node.getExternalEntry();
        auto scanFunc = externalEntry->getScanFunction();
        auto bindInput = function::TableFuncBindInput();
        auto bindData = scanFunc.bindFunc(clientContext, &bindInput);
        auto scanInfo = BoundFileScanInfo(scanFunc, std::move(bindData), node.getPropertyExprs());
        appendScanFile(&scanInfo, plan);
        // Join external table with internal table.
        auto joinCondition = std::make_pair(pkExpr, pkExpr);
        std::vector<join_condition_t> joinConditions;
        joinConditions.push_back(joinCondition);
        appendHashJoin(joinConditions, JoinType::INNER, nullptr, plan, buildPlan, plan);
        return ;
    }
    appendScanNodeTable(node.getInternalID(), node.getTableIDs(), properties, plan);
}

static std::vector<column_id_t> getColumnIDs(TableCatalogEntry* entry, const expression_vector& properties) {
    std::vector<column_id_t> columnIDs;
    for (auto& expr : properties) {
        auto& property = expr->constCast<PropertyExpression>();
        if (!property.hasPropertyID(entry->getTableID())) {
            columnIDs.push_back(INVALID_COLUMN_ID);
            continue ;
        }
        auto propertyID = property.getPropertyID(entry->getTableID());
        columnIDs.push_back(entry->getColumnID(propertyID));
    }
    return columnIDs;
}

void Planner::appendScanNodeTable(std::shared_ptr<Expression> nodeID,
    std::vector<table_id_t> tableIDs, const expression_vector& properties, LogicalPlan& plan) {
    auto propertiesToScan_ = removeInternalIDProperty(properties);
    std::vector<LogicalNodeTableScanInfo> tableScanInfos;
    for (auto& tableID : tableIDs) {
        auto entry = clientContext->getCatalog()->getTableCatalogEntry(clientContext->getTx(), tableID);
        tableScanInfos.emplace_back(tableID, getColumnIDs(entry, propertiesToScan_));
    }
    appendScanNodeTable(nodeID, propertiesToScan_, tableScanInfos, plan);
}

void Planner::appendScanNodeTable(std::shared_ptr<Expression> nodeID,
    const expression_vector& properties, const std::vector<LogicalNodeTableScanInfo>& tableScanInfos, LogicalPlan& plan) {
    auto scan = make_shared<LogicalScanNodeTable>(std::move(nodeID), properties, std::move(tableScanInfos));
    scan->computeFactorizedSchema();
    plan.setCardinality(cardinalityEstimator.estimateScanNode(scan.get()));
    plan.setLastOperator(std::move(scan));
}

} // namespace planner
} // namespace kuzu
