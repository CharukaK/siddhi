package io.siddhi.query.api.optimizer.beans;

import io.siddhi.query.api.expression.Expression;

import java.util.HashMap;

public class StreamJoinQueryBean {
    private InputFilterBean leftInputStream;
    private InputFilterBean rightInputStream;
    private Expression havingFilter;
    private Expression onCondition;
    private HashMap<String,String> selectionMap=new HashMap<>();

    public StreamJoinQueryBean(InputFilterBean leftInputStream, InputFilterBean rightInputStream, Expression havingFilter, Expression onCondition, HashMap<String, String> selectionMap) {
        this.leftInputStream = leftInputStream;
        this.rightInputStream = rightInputStream;
        this.havingFilter = havingFilter;
        this.onCondition = onCondition;
        this.selectionMap = selectionMap;
    }

    public InputFilterBean getLeftInputStream() {
        return leftInputStream;
    }

    public InputFilterBean getRightInputStream() {
        return rightInputStream;
    }

    public Expression getHavingFilter() {
        return havingFilter;
    }

    public Expression getOnCondition() {
        return onCondition;
    }


}
