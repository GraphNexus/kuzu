#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalVectorize : public LogicalOperator {
public:
    LogicalVectorize(std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::VECTORIZE, std::move(child)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override { return std::string{}; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalVectorize>(children[0]->copy());
    }
};

} // namespace planner
} // namespace kuzu
