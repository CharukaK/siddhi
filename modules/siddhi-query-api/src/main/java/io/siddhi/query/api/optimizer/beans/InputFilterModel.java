package io.siddhi.query.api.optimizer.beans;

import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.condition.And;

import java.util.List;

public class InputFilterModel {
    private String streamId;
    private String streamRefId;

    private List<String> attributes;

    private Expression filterExpression = null;

    public InputFilterModel(String streamId, String streamRefId, List<String> attributes) {
        this.streamId = streamId;
        this.streamRefId = streamRefId;
        this.attributes = attributes;
    }

    public String getStreamId() {
        return streamId;
    }

    public String getStreamRefId() {
        return streamRefId;
    }

    public void setStreamRefId(String streamRefId) {
        this.streamRefId = streamRefId;
    }

    public List<String> getAttributes() {
        return attributes;
    }

    public Expression getFilterExpression() {
        return filterExpression;
    }

    public void setFilterExpression(Expression filterExpression) {
        this.filterExpression = filterExpression;
    }

    public void addConjunctiveFilterExpression(Expression expression) {
        if (filterExpression == null) {
            filterExpression = expression;
        } else {
            filterExpression = new And(expression, filterExpression);
        }
    }

}
