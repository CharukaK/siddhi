package io.siddhi.query.api.optimizer2.beans.QueryModels;

import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.selection.OutputAttribute;
import io.siddhi.query.api.expression.Expression;

import java.util.HashMap;

public abstract class QueryModel {
    private Expression havingCondition;
    private boolean hasGroupBy;
    private boolean havingContainsFunctions;
    private HashMap<String, OutputAttribute> selectorAttribute = new HashMap<>();

    public Expression getHavingCondition() {
        return havingCondition;
    }

    public void setHavingCondition(Expression havingCondition) {
        this.havingCondition = havingCondition;
    }

    public boolean isHasGroupBy() {
        return hasGroupBy;
    }

    public void setHasGroupBy(boolean hasGroupBy) {
        this.hasGroupBy = hasGroupBy;
    }

    public boolean isHavingContainsFunctions() {
        return havingContainsFunctions;
    }

    public void setHavingContainsFunctions(boolean havingContainsFunctions) {
        this.havingContainsFunctions = havingContainsFunctions;
    }

    public HashMap<String, OutputAttribute> getSelectorAttribute() {
        return selectorAttribute;
    }

    public void setSelectorAttribute(HashMap<String, OutputAttribute> selectorAttribute) {
        this.selectorAttribute = selectorAttribute;
    }

    void addSelectorAttribute(String key, OutputAttribute attribute) {
        this.selectorAttribute.put(key, attribute);
    }

    public abstract void applyOptimization();

    public void reorderExpressionTree(Expression expression) {
        int leftDepth = 0;
        int rightDepth = 0;


    }

}
