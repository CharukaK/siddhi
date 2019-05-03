package io.siddhi.query.api.optimizer2.beans.InputModels;

import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.execution.query.input.handler.Filter;
import io.siddhi.query.api.execution.query.input.handler.StreamHandler;
import io.siddhi.query.api.execution.query.input.stream.InputStream;
import io.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.condition.And;

import java.util.HashMap;
import java.util.List;

public class StreamInputModel {
    private String streamId;
    private List<Attribute> attributes;
    private List<StreamHandler> streamFilters;
    private Expression filterExpression;
    private int windowPosition;

    public StreamInputModel(InputStream inputStream, HashMap<String, List<Attribute>> streamDefMap) {
        this.streamId = ((SingleInputStream) inputStream).getStreamId();
        this.attributes = streamDefMap.get(streamId);
        this.streamFilters = ((SingleInputStream) inputStream).getStreamHandlers();
        this.filterExpression = getFilterExpression(this.streamFilters);
        this.windowPosition = ((SingleInputStream) inputStream).getWindowPosition();
    }

    public String getStreamId() {
        return streamId;
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public Expression getFilterExpression() {
        return filterExpression;
    }

    public void addConjunctiveFilterExpression(Expression expression) {
        if (filterExpression == null) {
            filterExpression = expression;
        } else {
            filterExpression = new And(expression, filterExpression);
        }
    }

    public int getWindowPosition() {
        return windowPosition;
    }

    public void setWindowPosition(int windowPosition) {
        this.windowPosition = windowPosition;
    }

    public List<StreamHandler> getStreamFilters() {
        return streamFilters;
    }

    private Expression getFilterExpression(List<StreamHandler> streamHandlers) {
        return !streamHandlers.isEmpty() && streamHandlers.get(0) instanceof Filter ?
                ((Filter) streamHandlers.get(0)).getFilterExpression() : null;
    }

}
