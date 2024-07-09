#include "catalog/catalog_entry/node_table_reference_catalog_entry.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::transaction;

namespace kuzu {
namespace catalog {

void NodeTableReferenceCatalogEntry::serialize(Serializer& serializer) const {
    TableReferenceCatalogEntry::serialize(serializer);
    serializer.write(primaryKeyIdx);
}

std::unique_ptr<NodeTableReferenceCatalogEntry> NodeTableReferenceCatalogEntry::deserialize(Deserializer& deserializer) {
    common::idx_t primaryKeyIdx;
    deserializer.deserializeValue(primaryKeyIdx);
    auto entry = std::make_unique<NodeTableReferenceCatalogEntry>();
    entry->primaryKeyIdx = primaryKeyIdx;
    return entry;
}

// TODO: fixme
std::unique_ptr<TableCatalogEntry> NodeTableReferenceCatalogEntry::copy() const {
    return nullptr;
//    auto other = std::make_unique<NodeTableReferenceCatalogEntry>();
//    other->physicalEntry = physicalEntry->ptrCast<TableCatalogEntry>()->copy();
//    other->copyFrom(*this);
//    return other;
}

std::unique_ptr<BoundExtraCreateCatalogEntryInfo> NodeTableReferenceCatalogEntry::getBoundExtraCreateInfo(Transaction*) const {
    return nullptr;
    //    std::vector<PropertyInfo> propertyInfos;
//    for (const auto& property : properties) {
//        propertyInfos.emplace_back(property.getName(), property.getDataType().copy(),
//            property.getDefaultExpr()->copy());
//    }
//    return std::make_unique<BoundExtraCreateTableReferenceInfo>(std::move(propertyInfos),
//        primaryKeyIdx, externalDBName, externalTableName,
//        physicalEntry->ptrCast<TableCatalogEntry>()->getBoundCreateTableInfo(transaction));
}

}
}
