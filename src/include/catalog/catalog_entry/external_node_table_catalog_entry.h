#pragma once

#include "table_catalog_entry.h"

namespace kuzu {
namespace catalog {

class ExternalNodeTableCatalogEntry final : public TableCatalogEntry {
    static constexpr CatalogEntryType entryType = CatalogEntryType::FOREIGN_NODE_TABLE_ENTRY;

public:
    ExternalNodeTableCatalogEntry() = default;
    ExternalNodeTableCatalogEntry(CatalogSet* set, std::string name, common::table_id_t tableID,
        std::unique_ptr<CatalogEntry> physicalEntry)
        : TableCatalogEntry{set, entryType, std::move(name), tableID},
          physicalEntry{std::move(physicalEntry)} {}

    common::TableType getTableType() const override {
        return common::TableType::EXTERNAL_NODE;
    }

    std::string getExternalDBName() const {
        return externalDBName;
    }
    std::string getExternalTableName() const {
        return externalTableName;
    }
    CatalogEntry* getPhysicalEntry() const {
        return physicalEntry.get();
    }

    void serialize(common::Serializer &serializer) const override;
    static std::unique_ptr<ExternalNodeTableCatalogEntry> deserialize(common::Deserializer& deserializer);

    std::unique_ptr<TableCatalogEntry> copy() const override;

private:
    std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo> getBoundExtraCreateInfo(
        transaction::Transaction *transaction) const override;

private:
    common::idx_t primaryKeyIdx;
    //
    std::string externalDBName;
    std::string externalTableName;
    // nodeEntry stores the actual
    std::unique_ptr<CatalogEntry> physicalEntry;
};

}
}