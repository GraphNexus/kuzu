#include "function/lambda/lambda_function_bind_data.h"

namespace kuzu {
namespace function {

void LambdaFunctionBindData::setLambdaVarVector(evaluator::ExpressionEvaluator& evaluator,
    std::vector<std::shared_ptr<common::ValueVector>> varVector) {
    for (auto& child : evaluator.getChildren()) {
        if (child->isLambdaExpr()) {
            auto& lambdaEvaluator = child->cast<evaluator::LambdaExpressionEvaluator>();
            lambdaEvaluator.setResultVector(varVector.back());
            if (varVector.size() > 1) {
                varVector.pop_back();
            }
            continue;
        }
        setLambdaVarVector(*child, varVector);
    }
}

void LambdaFunctionBindData::initEvaluator(
    std::vector<std::shared_ptr<common::ValueVector>> resultVec,
    const processor::ResultSet& resultSet, main::ClientContext* clientContext) const {
    setLambdaVarVector(*evaluator, resultVec);
    evaluator->init(resultSet, clientContext);
}

std::unique_ptr<FunctionBindData> LambdaFunctionBindData::copy() const {
    auto result = std::make_unique<LambdaFunctionBindData>(copyVector(paramTypes),
        resultType.copy(), lambdaExpr->copy());
    result->evaluator = evaluator->clone();
    return result;
}

} // namespace function
} // namespace kuzu
