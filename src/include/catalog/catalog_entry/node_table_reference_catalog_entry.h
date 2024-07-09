#pragma once

#include "reference_catalog_entry.h"

namespace kuzu {
namespace catalog {

class NodeTableReferenceCatalogEntry final : public TableReferenceCatalogEntry {
    static constexpr CatalogEntryType entryType_ = CatalogEntryType::NODE_TABLE_REFERENCE_ENTRY;
public:
    NodeTableReferenceCatalogEntry() = default;
    NodeTableReferenceCatalogEntry(CatalogSet* set, std::string name, common::table_id_t tableID,
        std::string externalDBName, std::string externalTableName,
        std::unique_ptr<CatalogEntry> physicalEntry, common::idx_t primaryKeyIdx)
        : TableReferenceCatalogEntry{set, entryType_, std::move(name), tableID, externalDBName,
              externalTableName, std::move(physicalEntry)}, primaryKeyIdx{primaryKeyIdx} {}

    common::TableType getTableType() const override {
        return common::TableType::NODE_REFERENCE;
    }

    common::idx_t getPrimaryKeyIdx () const {
        return primaryKeyIdx;
    }

    void serialize(common::Serializer &serializer) const override;
    static std::unique_ptr<NodeTableReferenceCatalogEntry> deserialize(common::Deserializer& deserializer);

    std::unique_ptr<TableCatalogEntry> copy() const override;

private:
    std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo> getBoundExtraCreateInfo(
        transaction::Transaction *transaction) const override;

private:
    common::idx_t primaryKeyIdx;
};

}
}
