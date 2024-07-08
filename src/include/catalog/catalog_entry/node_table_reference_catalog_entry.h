#pragma once

#include "table_catalog_entry.h"

namespace kuzu {
namespace catalog {

class NodeTableReferenceCatalogEntry final : public TableCatalogEntry {
    static constexpr CatalogEntryType entryType = CatalogEntryType::NODE_TABLE_REFERENCE_ENTRY;

public:
    NodeTableReferenceCatalogEntry() = default;
    NodeTableReferenceCatalogEntry(CatalogSet* set, std::string name, common::table_id_t tableID,
        common::idx_t primaryKeyIdx, std::string externalDBName, std::string externalTableName,
        std::unique_ptr<CatalogEntry> physicalEntry)
        : TableCatalogEntry{set, entryType, std::move(name), tableID}, primaryKeyIdx{primaryKeyIdx},
          externalDBName{std::move(externalDBName)}, externalTableName{std::move(externalTableName)},
          physicalEntry{std::move(physicalEntry)} {}

    common::TableType getTableType() const override {
        return common::TableType::NODE_REFERENCE;
    }

    common::idx_t getPrimaryKeyIdx () const {
        return primaryKeyIdx;
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
    static std::unique_ptr<NodeTableReferenceCatalogEntry> deserialize(common::Deserializer& deserializer);

    std::unique_ptr<TableCatalogEntry> copy() const override;

private:
    std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo> getBoundExtraCreateInfo(
        transaction::Transaction *transaction) const override;

private:
    common::idx_t primaryKeyIdx;
    std::string externalDBName;
    std::string externalTableName;
    std::unique_ptr<CatalogEntry> physicalEntry;
};

}
}
