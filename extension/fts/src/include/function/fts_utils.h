#pragma once

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/types/value/value.h"
#include "main/client_context.h"

namespace kuzu {
namespace fts_extension {

struct FTSUtils {

    static catalog::NodeTableCatalogEntry& bindTable(const common::Value& tableName,
        main::ClientContext* context);

    static void validateIndexExistence(const catalog::NodeTableCatalogEntry& nodeTableCatalogEntry,
        std::string indexName);
};

} // namespace fts_extension
} // namespace kuzu
