package io.siddhi.query.api.optimizer.beans;

import io.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.List;

public class InputFilterBean {
    private String streamId;
    private List<String> attributes = new ArrayList<>();
    private Expression filterExpression;

    public InputFilterBean(String streamId, List<String> attributes) {
        this.streamId = streamId;
        this.attributes = attributes;
    }

    public String getStreamId() {
        return streamId;
    }

    public List<String> getAttributes() {
        return attributes;
    }

    public Expression getFilterExpression() {
        return filterExpression;
    }


}
