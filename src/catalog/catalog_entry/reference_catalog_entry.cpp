#include "catalog/catalog_entry/reference_catalog_entry.h"
#include "catalog/catalog_entry/node_table_reference_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_reference_catalog_entry.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void TableReferenceCatalogEntry::serialize(Serializer& serializer) const {
    TableCatalogEntry::serialize(serializer);
    serializer.write(externalDBName);
    serializer.write(externalTableName);
    serializer.serializeOptionalValue(physicalEntry);
}

std::unique_ptr<TableReferenceCatalogEntry> TableReferenceCatalogEntry::deserialize(Deserializer& deserializer, CatalogEntryType type) {
    std::string externalDBName;
    std::string externalTableName;
    std::unique_ptr<CatalogEntry> physicalEntry;
    deserializer.deserializeValue(externalDBName);
    deserializer.deserializeValue(externalTableName);
    deserializer.deserializeOptionalValue(physicalEntry);
    std::unique_ptr<TableReferenceCatalogEntry> entry;
    switch (type) {
    case CatalogEntryType::NODE_TABLE_REFERENCE_ENTRY: {
        entry = NodeTableReferenceCatalogEntry::deserialize(deserializer);
    } break ;
    case CatalogEntryType::REL_TABLE_REFERENCE_ENTRY: {
        entry = RelTableReferenceCatalogEntry::deserialize(deserializer);
    } break ;
    default:
        KU_UNREACHABLE;
    }
    entry->externalDBName = externalDBName;
    entry->externalTableName = externalTableName;
    entry->physicalEntry = std::move(physicalEntry);
    return entry;
}

}
}
