#include "planner/operator/logical_vectorize.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void LogicalVectorize::computeFactorizedSchema() {
  createEmptySchema();
  uint32_t groupPos = schema->createGroup();
  auto childSchema = getChild(0)->getSchema();
  auto numGroups = childSchema->getNumGroups();
  for (auto i = 0u; i < numGroups; i++) {
    for (auto expr: childSchema->getGroup(i)->getExpressions()) {
      schema->insertToGroupAndScope(expr, groupPos);
    }
  }
}

void LogicalVectorize::computeFlatSchema() {
  createEmptySchema();
  uint32_t groupPos = schema->createGroup();
  auto childSchema = getChild(0)->getSchema();
  auto numGroups = childSchema->getNumGroups();
  for (auto i = 0u; i < numGroups; i++) {
    for (auto expr: childSchema->getGroup(i)->getExpressions()) {
      schema->insertToGroupAndScope(expr, groupPos);
    }
  }
}

} // namespace planner
} // namespace kuzu
