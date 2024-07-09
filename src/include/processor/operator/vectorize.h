#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Vectorize : public PhysicalOperator {
  static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::VECTORIZE;

public:
  Vectorize(std::unique_ptr<PhysicalOperator> child,
    uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
    : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)} {}

  bool getNextTuplesInternal(ExecutionContext* context) override;

  inline std::unique_ptr<PhysicalOperator> clone() override {
    return make_unique<Vectorize>(children[0]->clone(), id,
      printInfo->copy());
  }
};

} // namespace processor
} // namespace kuzu
