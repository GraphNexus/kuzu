#include "function/lambda/lambda_function_bind_data.h"
#include "function/list/vector_list_functions.h"

namespace kuzu {
namespace function {

using namespace common;

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* function) {
    std::vector<LogicalType> paramTypes;
    auto returnType = common::ListType::getChildType(arguments[0]->getDataType()).copy();
    paramTypes.push_back(arguments[0]->getDataType().copy());
    function->ptrCast<ScalarFunction>()->returnTypeID = returnType.getLogicalTypeID();
    return std::make_unique<LambdaFunctionBindData>(std::move(paramTypes), std::move(returnType),
        arguments[1]);
}

static void execute(const std::vector<std::shared_ptr<common::ValueVector>>& operand,
    common::ValueVector& result, void* dataPtr) {
    auto lambdaFuncBindData = reinterpret_cast<LambdaFunctionBindData*>(dataPtr);
    KU_ASSERT(operand.size() == 1);

    auto xVector = std::make_shared<common::ValueVector>(result.dataType.copy(),
        lambdaFuncBindData->clientContext->getMemoryManager());
    auto yVector = std::make_shared<common::ValueVector>(result.dataType.copy(),
        lambdaFuncBindData->clientContext->getMemoryManager());

    lambdaFuncBindData->setLambdaVarVector(*lambdaFuncBindData->evaluator, {xVector, yVector});
    auto dataVec = common::ListVector::getSharedDataVector(operand[0].get());
    dataVec->state->getSelVectorUnsafe().setSelSize(
        common::ListVector::getDataVectorSize(operand[0].get()));
    lambdaFuncBindData->evaluator->evaluate();
    result.state = operand[0]->state;
    auto tmpResult = lambdaFuncBindData->evaluator->resultVector;
    auto a = tmpResult->getValue<int64_t>(0);
    auto b = tmpResult->getValue<int64_t>(1);
    auto c = 5;
}

function_set ListReduceFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::ANY,
        execute);
    function->bindFunc = bindFunc;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
