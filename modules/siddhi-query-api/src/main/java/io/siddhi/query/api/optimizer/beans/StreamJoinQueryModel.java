package io.siddhi.query.api.optimizer.beans;

import io.siddhi.query.api.execution.query.selection.OutputAttribute;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.math.Add;

import java.util.HashMap;
import java.util.List;

public class StreamJoinQueryModel {
    private InputFilterModel leftInputStream;
    private InputFilterModel rightInputStream;
    private Expression havingFilter;
    private Expression onCondition;
    private HashMap<String,String> selectionMap=new HashMap<>();

    public StreamJoinQueryModel(InputFilterModel leftInputStream, InputFilterModel rightInputStream, List<OutputAttribute> selectionList) {
        this.leftInputStream = leftInputStream;
        this.rightInputStream = rightInputStream;
        selectionList.forEach(entry -> this.selectionMap.put(entry.getRename(),
                (entry.getExpression() instanceof Variable ? ((Variable)entry.getExpression()).getAttributeName():"Attribute Function")));
    }

    public InputFilterModel getLeftInputStream() {
        return leftInputStream;
    }

    public InputFilterModel getRightInputStream() {
        return rightInputStream;
    }

    public Expression getHavingFilter() {
        return havingFilter;
    }

    public Expression getOnCondition() {
        return onCondition;
    }

    public void addConjunctiveHavingCondition(Expression expression){
        if(havingFilter==null) {
            havingFilter = expression;
        } else {
            havingFilter = new Add(expression,havingFilter);
        }
    }

    public void addConjunctiveOnCondition(Expression expression){
        if(onCondition==null) {
            onCondition = expression;
        } else {
            onCondition = new Add(expression,havingFilter);
        }
    }


}
