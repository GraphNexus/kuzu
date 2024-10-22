#include "parser/visitor/standalone_call_analyzer.h"

#include "catalog/catalog.h"
#include "common/enums/expression_type.h"
#include "function/built_in_function_utils.h"
#include "main/client_context.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/expression/parsed_literal_expression.h"
#include "parser/parsed_expression_visitor.h"
#include "parser/standalone_call_function.h"
#include "binder/binder.h"

namespace kuzu {
namespace parser {

std::string StandaloneCallAnalyzer::getRewriteQuery(const Statement& statement) {
    visit(statement);
    return rewriteQuery;
}

void StandaloneCallAnalyzer::visitStandaloneCallFunction(const Statement& statement) {
    auto& standaloneCallFunc = statement.constCast<StandaloneCallFunction>();
    auto& funcExpr =
        standaloneCallFunc.getFunctionExpression()->constCast<ParsedFunctionExpression>();
    auto catalogSet = context->getCatalog()->getFunctions(context->getTx());
    auto entry = function::BuiltInFunctionsUtils::getFunctionCatalogEntry(context->getTx(),
        funcExpr.getFunctionName(), catalogSet);
    std::vector<common::LogicalType> childrenTypes;
    binder::Binder binder{context};
    for (auto i = 0u; i < funcExpr.getNumChildren(); i++) {
        auto child = funcExpr.getChild(i);
        ParsedExpressionUtils::validateType(*child, common::ExpressionType::LITERAL);
        auto literalExpr = child->constPtrCast<ParsedLiteralExpression>();
        childrenTypes.push_back(literalExpr->getValue().getDataType().copy());
    }
    auto func = function::BuiltInFunctionsUtils::matchFunction(funcExpr.getFunctionName(),
        childrenTypes, entry);
    auto standaloneCallFunction = func->constPtrCast<function::StandaloneCallFunction>();
    ParsedExpressionChildrenVisitor visitor;
    rewriteQuery = standaloneCallFunction->rewriteFunc(*context, visitor.collectChildren(funcExpr));
}

} // namespace parser
} // namespace kuzu
