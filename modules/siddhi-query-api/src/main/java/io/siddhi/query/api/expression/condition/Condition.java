package io.siddhi.query.api.expression.condition;

import io.siddhi.query.api.expression.Expression;

public interface Condition {
    Expression getRightExpression();
    Expression getLeftExpression();

    void setRightExpression(Expression expression);
    void setLeftExpression(Expression expression);
}
