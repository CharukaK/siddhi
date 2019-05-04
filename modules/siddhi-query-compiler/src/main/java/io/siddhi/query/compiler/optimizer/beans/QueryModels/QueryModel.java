package io.siddhi.query.compiler.optimizer.beans.QueryModels;

import io.siddhi.query.api.execution.query.selection.OutputAttribute;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.condition.Condition;
import io.siddhi.query.api.expression.condition.Not;

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


    /**
     * Method to reorder the conditionTrees so that the lowest depth is in the leftbranch has the lowest depth
     * @param expression
     * @param depth
     * @return
     */
    public int reorderExpressionTree(Expression expression, int depth) {
        int leftDepth = 0;
        int rightDepth = 0;

        if (expression instanceof Condition) {
            leftDepth = reorderExpressionTree(((Condition) expression).getLeftExpression(), depth + 1);
            rightDepth = reorderExpressionTree(((Condition) expression).getRightExpression(), depth + 1);

            if(leftDepth>rightDepth) {
                Expression leftExpression = ((Condition) expression).getLeftExpression();
                ((Condition) expression).setLeftExpression(((Condition) expression).getRightExpression());
                ((Condition) expression).setRightExpression(leftExpression);
            }
        }

        if (expression instanceof Not) {
            return reorderExpressionTree(expression, depth+1);
        }

        return (leftDepth == 0 && rightDepth == 0 ?
                depth + 1 : (leftDepth > rightDepth ?
                                        leftDepth + 1 : rightDepth + 1));
    }

}
