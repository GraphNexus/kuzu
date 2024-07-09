#pragma once

#include "table_catalog_entry.h"

namespace kuzu {
namespace catalog {

class TableReferenceCatalogEntry : public TableCatalogEntry {
public:
    TableReferenceCatalogEntry() = default;
    TableReferenceCatalogEntry(CatalogSet* set, CatalogEntryType entryType, std::string name,
        common::table_id_t tableID, std::string externalDBName, std::string externalTableName,
        std::unique_ptr<CatalogEntry> physicalEntry)
        : TableCatalogEntry{set, entryType, std::move(name), tableID},
          externalDBName{std::move(externalDBName)}, externalTableName{std::move(externalTableName)},
          physicalEntry{std::move(physicalEntry)}  {}

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
    static std::unique_ptr<TableReferenceCatalogEntry> deserialize(common::Deserializer& deserializer, CatalogEntryType type);

protected:
    std::string externalDBName;
    std::string externalTableName;
    std::unique_ptr<CatalogEntry> physicalEntry;
};

}
}
