#include "catalog/catalog_entry/external_node_table_catalog_entry.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::transaction;

namespace kuzu {
namespace catalog {

void ExternalNodeTableCatalogEntry::serialize(Serializer& serializer) const {
    TableCatalogEntry::serialize(serializer);
    serializer.serializeOptionalValue(physicalEntry);
}

std::unique_ptr<ExternalNodeTableCatalogEntry> ExternalNodeTableCatalogEntry::deserialize(Deserializer& deserializer) {
    std::unique_ptr<NodeTableCatalogEntry> physicalEntry;
    deserializer.deserializeOptionalValue(physicalEntry);
    auto entry = std::make_unique<ExternalNodeTableCatalogEntry>();
    entry->physicalEntry = std::move(physicalEntry);
    return entry;
}

std::unique_ptr<TableCatalogEntry> ExternalNodeTableCatalogEntry::copy() const {
    auto other = std::make_unique<ExternalNodeTableCatalogEntry>();
    other->physicalEntry = physicalEntry->copy();
    other->copyFrom(*this);
    return other;
}

std::unique_ptr<BoundExtraCreateCatalogEntryInfo> ExternalNodeTableCatalogEntry::getBoundExtraCreateInfo(Transaction* transaction) const {
    std::vector<PropertyInfo> propertyInfos;
    for (const auto& property : properties) {
        propertyInfos.emplace_back(property.getName(), property.getDataType().copy(),
            property.getDefaultExpr()->copy());
    }
    return std::make_unique<BoundExtraCreateExternalNodeTableInfo>(std::move(propertyInfos),
        physicalEntry->getBoundCreateTableInfo(transaction));
}

}
}