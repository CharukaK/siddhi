package io.siddhi.query.api.expression.math;

import io.siddhi.query.api.expression.Expression;

public interface Operation {
    Expression getLeftValue();
    Expression getRightValue();
}
