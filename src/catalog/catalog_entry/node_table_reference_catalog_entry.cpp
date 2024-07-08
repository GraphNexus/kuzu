#include "catalog/catalog_entry/node_table_reference_catalog_entry.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::transaction;

namespace kuzu {
namespace catalog {

void NodeTableReferenceCatalogEntry::serialize(Serializer& serializer) const {
    TableCatalogEntry::serialize(serializer);
    serializer.write(primaryKeyIdx);
    serializer.write(externalDBName);
    serializer.write(externalTableName);
    serializer.serializeOptionalValue(physicalEntry);
}

std::unique_ptr<NodeTableReferenceCatalogEntry> NodeTableReferenceCatalogEntry::deserialize(Deserializer& deserializer) {
    common::idx_t primaryKeyIdx;
    std::string externalDBName;
    std::string externalTableName;
    std::unique_ptr<NodeTableCatalogEntry> physicalEntry;
    deserializer.deserializeValue(primaryKeyIdx);
    deserializer.deserializeValue(externalDBName);
    deserializer.deserializeValue(externalTableName);
    deserializer.deserializeOptionalValue(physicalEntry);
    auto entry = std::make_unique<NodeTableReferenceCatalogEntry>();
    entry->primaryKeyIdx = primaryKeyIdx;
    entry->externalDBName = externalDBName;
    entry->externalTableName = externalTableName;
    entry->physicalEntry = std::move(physicalEntry);
    return entry;
}

std::unique_ptr<TableCatalogEntry> NodeTableReferenceCatalogEntry::copy() const {
    auto other = std::make_unique<NodeTableReferenceCatalogEntry>();
    other->physicalEntry = physicalEntry->ptrCast<TableCatalogEntry>()->copy();
    other->copyFrom(*this);
    return other;
}

std::unique_ptr<BoundExtraCreateCatalogEntryInfo> NodeTableReferenceCatalogEntry::getBoundExtraCreateInfo(Transaction* transaction) const {
    std::vector<PropertyInfo> propertyInfos;
    for (const auto& property : properties) {
        propertyInfos.emplace_back(property.getName(), property.getDataType().copy(),
            property.getDefaultExpr()->copy());
    }
    return std::make_unique<BoundExtraCreateNodeTableReferenceInfo>(std::move(propertyInfos),
        primaryKeyIdx, externalDBName, externalTableName,
        physicalEntry->ptrCast<TableCatalogEntry>()->getBoundCreateTableInfo(transaction));
}

}
}
