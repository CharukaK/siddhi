package io.siddhi.query.compiler.optimizer.beans.QueryModels;

import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.expression.AttributeFunction;
import io.siddhi.query.compiler.optimizer.beans.InputModels.StreamInputModel;

import java.util.HashMap;
import java.util.List;

public class SPQueryModel extends QueryModel {
    private StreamInputModel inputModel;

    public SPQueryModel(Query query, HashMap<String, List<Attribute>> streamMap) {
        this.inputModel = new StreamInputModel(query.getInputStream(), streamMap);
        setHavingCondition(query.getSelector().getHavingExpression());
        setHasGroupBy(query.getSelector().getGroupByList().isEmpty());
        setHavingContainsFunctions(query.getSelector()
                .getSelectionList().stream().anyMatch(element -> element.getExpression() instanceof AttributeFunction));

        query.getSelector().getSelectionList().forEach(element -> addSelectorAttribute(element.getRename(), element));
    }

    public StreamInputModel getInputModel() {
        return inputModel;
    }

    @Override
    public void applyOptimization() {
        reorderExpressionTree(inputModel.getFilterExpression(), 0);
        reorderExpressionTree(getHavingCondition(), 0);

        inputModel.updateStreamHandlers();
    }
}
