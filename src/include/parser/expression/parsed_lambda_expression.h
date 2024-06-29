#pragma once

#include "common/enums/expression_type.h"
#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedLambdaExpression : public ParsedExpression {
public:
    static constexpr const common::ExpressionType TYPE = common::ExpressionType::LAMBDA;

public:
    ParsedLambdaExpression(std::vector<std::unique_ptr<ParsedExpression>> variables,
        std::unique_ptr<ParsedExpression> expr, std::string rawName)
        : ParsedExpression{TYPE, rawName}, variables{std::move(variables)}, expr{std::move(expr)} {}

    std::vector<ParsedExpression*> getVariables() const {
        std::vector<ParsedExpression*> parsedExpressions;
        for (auto& variable : variables) {
            parsedExpressions.push_back(variable.get());
        }
        return parsedExpressions;
    }

    ParsedExpression* getExpr() const { return expr.get(); }

private:
    std::vector<std::unique_ptr<ParsedExpression>> variables;
    std::unique_ptr<ParsedExpression> expr;
};

} // namespace parser
} // namespace kuzu
