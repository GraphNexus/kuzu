#include "common/enums/table_type.h"

#include "common/assert.h"

namespace kuzu {
namespace common {

std::string TableTypeUtils::toString(TableType tableType) {
    switch (tableType) {
    case TableType::UNKNOWN: {
        return "UNKNOWN";
    }
    case TableType::NODE: {
        return "NODE";
    }
    case TableType::REL: {
        return "REL";
    }
    case TableType::RDF: {
        return "RDFGRAPH";
    }
    case TableType::REL_GROUP: {
        return "REL_GROUP";
    }
    case TableType::EXTERNAL: {
        return "EXTERNAL";
    }
    case TableType::NODE_REFERENCE: {
        return "NODE_REFERENCE";
    }
    case TableType::REL_REFERENCE: {
        return "REL_REFERENCE";
    }
    default:
        KU_UNREACHABLE;
    }
}

} // namespace common
} // namespace kuzu
