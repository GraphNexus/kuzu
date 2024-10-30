#include "catalog/catalog_entry/index_catalog_entry.h"

namespace kuzu {
namespace catalog {

using namespace kuzu::common;

void IndexCatalogEntry::copyFrom(const CatalogEntry& other) {
    CatalogEntry::copyFrom(other);
    auto& otherTable = other.constCast<IndexCatalogEntry>();
    tableID = otherTable.tableID;
    indexName = otherTable.indexName;
}

} // namespace catalog
} // namespace kuzu
