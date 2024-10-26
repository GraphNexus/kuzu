#include "function/fts_utils.h"

#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "common/types/value/nested.h"

namespace kuzu {
namespace fts_extension {

catalog::NodeTableCatalogEntry& FTSUtils::bindTable(const common::Value& tableName,
    main::ClientContext* context, std::string indexName, IndexOperation operation) {
    if (!context->getCatalog()->containsTable(context->getTx(), tableName.toString())) {
        throw common::BinderException{
            common::stringFormat("Table {} does not exist.", tableName.toString())};
    }
    auto tableEntry =
        context->getCatalog()->getTableCatalogEntry(context->getTx(), tableName.toString());
    if (tableEntry->getTableType() != common::TableType::NODE) {
        switch (operation) {
        case IndexOperation::CREATE:
            throw common::BinderException{
                common::stringFormat("Table: {} is not a node table. Can only build full text "
                                     "search index on node tables.",
                    tableEntry->getName())};
        case IndexOperation::QUERY:
        case IndexOperation::DROP:
            throw common::BinderException{common::stringFormat(
                "Table: {} doesn't have an index with name: {}. Only node tables can "
                "have full text search indexes.",
                tableEntry->getName(), indexName)};
        }
    }
    auto nodeTableEntry = tableEntry->ptrCast<catalog::NodeTableCatalogEntry>();
    return *nodeTableEntry;
}

void FTSUtils::validateIndexExistence(const catalog::NodeTableCatalogEntry& nodeTableCatalogEntry,
    std::string indexName) {
    if (!nodeTableCatalogEntry.containsIndex(indexName)) {
        throw common::BinderException{
            common::stringFormat("Table: {} doesn't have an index with name: {}.",
                nodeTableCatalogEntry.getName(), indexName)};
    }
}

} // namespace fts_extension
} // namespace kuzu
