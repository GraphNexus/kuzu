#include "planner/operator/logical_vectorize.h"
#include "processor/operator/vectorize.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapVectorize(LogicalOperator* logicalOperator) {
    auto& vectorize = logicalOperator->constCast<LogicalVectorize>();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto printInfo = std::make_unique<OPPrintInfo>(vectorize.getExpressionsForPrinting());
    return make_unique<Vectorize>(std::move(prevOperator), getOperatorID(), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
