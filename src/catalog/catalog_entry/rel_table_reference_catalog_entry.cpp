#include "catalog/catalog_entry/rel_table_reference_catalog_entry.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::transaction;

namespace kuzu {
namespace catalog {

void RelTableReferenceCatalogEntry::serialize(common::Serializer& serializer) const {
    TableReferenceCatalogEntry::serialize(serializer);
    serializer.write(srcTableID);
    serializer.write(dstTableID);
}

std::unique_ptr<RelTableReferenceCatalogEntry> RelTableReferenceCatalogEntry::deserialize(Deserializer& deserializer) {
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
    deserializer.deserializeValue(srcTableID);
    deserializer.deserializeValue(dstTableID);
    auto entry = std::make_unique<RelTableReferenceCatalogEntry>();
    entry->srcTableID = srcTableID;
    entry->dstTableID = dstTableID;
    return entry;
}

std::unique_ptr<TableCatalogEntry> RelTableReferenceCatalogEntry::copy() const {

}

std::unique_ptr<BoundExtraCreateCatalogEntryInfo> RelTableReferenceCatalogEntry::getBoundExtraCreateInfo(transaction::Transaction* transaction) const {

}

}
}
