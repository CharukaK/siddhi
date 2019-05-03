package io.siddhi.query.api.optimizer2.beans.QueryModels;

import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.expression.AttributeFunction;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.optimizer2.beans.InputModels.StreamInputModel;

import java.util.HashMap;
import java.util.List;

public class SPJQueryModel extends QueryModel {
    private StreamInputModel leftInputModel;
    private StreamInputModel rightInputModel;
    private Expression onConditionExpression;


    public SPJQueryModel(Query joinQuery, HashMap<String, List<Attribute>> streamMap) {
        this.leftInputModel = new StreamInputModel(
                ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream(), streamMap);
        this.rightInputModel = new StreamInputModel(
                ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream(), streamMap);
        this.onConditionExpression = ((JoinInputStream) joinQuery.getInputStream()).getOnCompare();
        setHavingCondition(joinQuery.getSelector().getHavingExpression());
        setHasGroupBy(joinQuery.getSelector().getGroupByList().isEmpty());
        setHavingContainsFunctions(joinQuery.getSelector()
                .getSelectionList().stream().anyMatch(element -> element.getExpression() instanceof AttributeFunction));

        joinQuery.getSelector().getSelectionList().forEach(element -> addSelectorAttribute(element.getRename(), element));
    }

    @Override
    public void applyOptimization() {

    }
}
