#include "processor/operator/vectorize.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

bool Vectorize::getNextTuplesInternal(ExecutionContext* context) {
  if (!children[0]->getNextTuple(context)) {
    return false;
  }
  return true;
}

} // namespace processor
} // namespace kuzu
