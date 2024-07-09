#pragma once

#include "binder/expression/expression.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"

namespace kuzu {
namespace binder {

struct IndexLookupInfo {
    catalog::NodeTableCatalogEntry* nodeEntry;
    std::shared_ptr<binder::Expression> offset; // output
    std::shared_ptr<binder::Expression> key;    // input

    IndexLookupInfo(catalog::NodeTableCatalogEntry* nodeEntry, std::shared_ptr<binder::Expression> offset,
        std::shared_ptr<binder::Expression> key)
        : nodeEntry{nodeEntry}, offset{std::move(offset)}, key{std::move(key)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(IndexLookupInfo);

private:
    IndexLookupInfo(const IndexLookupInfo& other)
        :  nodeEntry{other.nodeEntry}, offset{other.offset}, key{other.key} {}
};

} // namespace binder
} // namespace kuzu
