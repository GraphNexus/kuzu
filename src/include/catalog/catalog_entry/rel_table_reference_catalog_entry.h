#pragma once

#include "reference_catalog_entry.h"

namespace kuzu {
namespace catalog {

class RelTableReferenceCatalogEntry final : public TableReferenceCatalogEntry {
    static constexpr CatalogEntryType entryType_ = CatalogEntryType::REL_TABLE_REFERENCE_ENTRY;
public:
    RelTableReferenceCatalogEntry() = default;
    RelTableReferenceCatalogEntry(CatalogSet* set, std::string name, common::table_id_t tableID,
        std::string externalDBName, std::string externalTableName,
        std::unique_ptr<CatalogEntry> physicalEntry, common::table_id_t srcTableID,
        common::table_id_t dstTableID)
        : TableReferenceCatalogEntry{set, entryType_, std::move(name), tableID, externalDBName,
              externalTableName, std::move(physicalEntry)}, srcTableID{srcTableID},
          dstTableID{dstTableID} {}

    common::TableType getTableType() const override {
        return common::TableType::REL_REFERENCE;
    }

    common::table_id_t getSrcTableID() const {
        return srcTableID;
    }
    common::table_id_t getDstTableID() const {
        return dstTableID;
    }

    void serialize(common::Serializer &serializer) const override;
    static std::unique_ptr<RelTableReferenceCatalogEntry> deserialize(common::Deserializer& deserializer);

    std::unique_ptr<TableCatalogEntry> copy() const override;

private:
    std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo> getBoundExtraCreateInfo(transaction::Transaction *transaction) const override;

private:
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
};


}
}