package io.siddhi.query.compiler.optimizer.beans.QueryModels;

import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.expression.AttributeFunction;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.condition.And;
import io.siddhi.query.api.expression.condition.Compare;
import io.siddhi.query.api.expression.condition.Condition;
import io.siddhi.query.api.expression.math.Operation;
import io.siddhi.query.compiler.optimizer.beans.InputModels.StreamInputModel;

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
                ((JoinInputStream) joinQuery.getInputStream()).getRightInputStream(), streamMap);
        this.onConditionExpression = ((JoinInputStream) joinQuery.getInputStream()).getOnCompare();
        setHavingCondition(joinQuery.getSelector().getHavingExpression());
        setHasGroupBy(joinQuery.getSelector().getGroupByList().isEmpty());
        setHavingContainsFunctions(joinQuery.getSelector()
                .getSelectionList().stream().anyMatch(element -> element.getExpression() instanceof AttributeFunction));

        joinQuery.getSelector().getSelectionList().forEach(element -> addSelectorAttribute(element.getRename(), element));
    }

    public StreamInputModel getLeftInputModel() {
        return leftInputModel;
    }

    public StreamInputModel getRightInputModel() {
        return rightInputModel;
    }

    public Expression getOnConditionExpression() {
        return onConditionExpression;
    }

    @Override
    public void applyOptimization() {
        seperateSelectionOfOnCondition(onConditionExpression, leftInputModel, rightInputModel, 0);

        reorderExpressionTree(onConditionExpression, 0);
        reorderExpressionTree(leftInputModel.getFilterExpression(), 0);
        reorderExpressionTree(rightInputModel.getFilterExpression(), 0);
        reorderExpressionTree(getHavingCondition(), 0);

        leftInputModel.updateStreamHandlers();
        rightInputModel.updateStreamHandlers();
    }

    private boolean seperateSelectionOfOnCondition(Expression expression, StreamInputModel leftInputModel, StreamInputModel rightInputModel, int depth) {
        boolean keepLeft =false;
        boolean keepRight = false;

        if (expression instanceof And) {
            keepLeft = seperateSelectionOfOnCondition(((And) expression).getLeftExpression(), leftInputModel, rightInputModel,depth+1);
            keepRight = seperateSelectionOfOnCondition(((And) expression).getRightExpression(), leftInputModel, rightInputModel, depth+1);

            if (!keepLeft && !keepRight) {
                // move selection
                moveSelectionOfOnCondition(((And) expression).getLeftExpression(), leftInputModel, rightInputModel);
                moveSelectionOfOnCondition(((And) expression).getRightExpression(), leftInputModel, rightInputModel);
            } else if (!keepLeft) {
                // move only left selection
                moveSelectionOfOnCondition(((And) expression).getLeftExpression(), leftInputModel, rightInputModel);
                if(depth==0) {
                    this.onConditionExpression = ((Condition) expression).getRightExpression();
                } else {
                    expression = ((Condition) expression).getRightExpression();
                }

            } else if (!keepRight) {
                // move only right selection
                moveSelectionOfOnCondition(((And) expression).getRightExpression(), leftInputModel, rightInputModel);
                if(depth==0) {
                    this.onConditionExpression = ((Condition) expression).getLeftExpression();
                } else {
                    expression = ((Condition) expression).getLeftExpression();
                }
            } else {
                if(depth==0) {
                    this.onConditionExpression= expression;
                }
            }


        } else if (expression instanceof Compare) {
            if (((Compare) expression).getLeftExpression() instanceof Variable && ((Compare) expression).getRightExpression() instanceof Variable) {
                return true;
            }

            if (((Compare) expression).getLeftExpression() instanceof Operation && ((Compare) expression).getRightExpression() instanceof Operation) {
                return true;
            }

            return (((Compare) expression).getLeftExpression() instanceof Operation || ((Compare) expression).getRightExpression() instanceof Operation) &&
                    (((Compare) expression).getLeftExpression() instanceof Variable || ((Compare) expression).getRightExpression() instanceof Variable);

        }

        return keepLeft || keepRight;
    }


    private void moveSelectionOfOnCondition(Expression expression, StreamInputModel leftInputModel, StreamInputModel rightInputModel) {
        if (expression instanceof Compare) {
            if (((Compare) expression).getLeftExpression() instanceof Variable) {
                if (((Variable) ((Compare) expression).getLeftExpression()).getStreamId().equals(leftInputModel.getStreamId()) ||
                        ((Variable) ((Compare) expression).getLeftExpression()).getStreamId().equals(leftInputModel.getStreamRefId())) {

                    ((Variable) ((Compare) expression).getLeftExpression()).setStreamId(null);
                    leftInputModel.addConjunctiveFilterExpression(expression);
                } else {

                    ((Variable) ((Compare) expression).getLeftExpression()).setStreamId(null);
                    rightInputModel.addConjunctiveFilterExpression(expression);
                }
            } else if (((Compare) expression).getLeftExpression() instanceof Operation) {

                if (((Operation) ((Compare) expression).getLeftExpression()).getLeftValue() instanceof Variable) {
                    if (((Variable) ((Operation) ((Compare) expression).getLeftExpression()).getLeftValue()).getStreamId().equals(leftInputModel.getStreamId()) ||
                            ((Variable) ((Operation) ((Compare) expression).getLeftExpression()).getLeftValue()).getStreamId().equals(leftInputModel.getStreamRefId())) {

                        ((Variable) ((Operation) ((Compare) expression).getLeftExpression()).getLeftValue()).setStreamId(null);
                        leftInputModel.addConjunctiveFilterExpression(expression);
                    } else {

                        ((Variable) ((Operation) ((Compare) expression).getLeftExpression()).getLeftValue()).setStreamId(null);
                        rightInputModel.addConjunctiveFilterExpression(expression);
                    }

                } else {
                    if (((Variable) ((Operation) ((Compare) expression).getLeftExpression()).getRightValue()).getStreamId().equals(leftInputModel.getStreamId()) ||
                            ((Variable) ((Operation) ((Compare) expression).getLeftExpression()).getRightValue()).getStreamId().equals(leftInputModel.getStreamRefId())) {

                        ((Variable) ((Operation) ((Compare) expression).getLeftExpression()).getRightValue()).setStreamId(null);
                        leftInputModel.addConjunctiveFilterExpression(expression);
                    } else {

                        ((Variable) ((Operation) ((Compare) expression).getLeftExpression()).getRightValue()).setStreamId(null);
                        rightInputModel.addConjunctiveFilterExpression(expression);
                    }
                }

            } else if (((Compare) expression).getRightExpression() instanceof Variable) {
                if (((Variable) ((Compare) expression).getRightExpression()).getStreamId().equals(leftInputModel.getStreamId()) ||
                        ((Variable) ((Compare) expression).getRightExpression()).getStreamId().equals(leftInputModel.getStreamRefId())) {

                    ((Variable) ((Compare) expression).getRightExpression()).setStreamId(null);
                    leftInputModel.addConjunctiveFilterExpression(expression);
                } else {

                    ((Variable) ((Compare) expression).getRightExpression()).setStreamId(null);
                    rightInputModel.addConjunctiveFilterExpression(expression);
                }
            } else {
                if (((Operation) ((Compare) expression).getRightExpression()).getLeftValue() instanceof Variable) {
                    if (((Variable) ((Operation) ((Compare) expression).getRightExpression()).getLeftValue()).getStreamId().equals(leftInputModel.getStreamId()) ||
                            ((Variable) ((Operation) ((Compare) expression).getRightExpression()).getLeftValue()).getStreamId().equals(leftInputModel.getStreamRefId())) {

                        ((Variable) ((Operation) ((Compare) expression).getRightExpression()).getLeftValue()).setStreamId(null);
                        leftInputModel.addConjunctiveFilterExpression(expression);
                    } else {

                        ((Variable) ((Operation) ((Compare) expression).getRightExpression()).getLeftValue()).setStreamId(null);
                        rightInputModel.addConjunctiveFilterExpression(expression);
                    }

                } else {
                    if (((Variable) ((Operation) ((Compare) expression).getRightExpression()).getRightValue()).getStreamId().equals(leftInputModel.getStreamId()) ||
                            ((Variable) ((Operation) ((Compare) expression).getRightExpression()).getRightValue()).getStreamId().equals(leftInputModel.getStreamRefId())) {

                        ((Variable) ((Operation) ((Compare) expression).getRightExpression()).getRightValue()).setStreamId(null);
                        leftInputModel.addConjunctiveFilterExpression(expression);
                    } else {

                        ((Variable) ((Operation) ((Compare) expression).getRightExpression()).getRightValue()).setStreamId(null);
                        rightInputModel.addConjunctiveFilterExpression(expression);
                    }
                }
            }

        }
    }

}
