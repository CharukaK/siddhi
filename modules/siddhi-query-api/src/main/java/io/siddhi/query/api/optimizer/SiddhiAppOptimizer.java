package io.siddhi.query.api.optimizer;

import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.execution.ExecutionElement;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.handler.Filter;
import io.siddhi.query.api.execution.query.input.stream.BasicSingleInputStream;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import io.siddhi.query.api.execution.query.selection.OutputAttribute;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.condition.*;
import io.siddhi.query.api.expression.math.Operation;
import io.siddhi.query.api.optimizer.beans.InputFilterModel;
import io.siddhi.query.api.optimizer.beans.StreamJoinQueryModel;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class SiddhiAppOptimizer {
    private HashMap<String, List<String>> completeStreamMap = new HashMap<>();
    private SiddhiApp siddhiApp;

    public SiddhiAppOptimizer(SiddhiApp siddhiApp) {
        this.siddhiApp = siddhiApp;
        extractCompleteStreamMap(siddhiApp);
    }

    public SiddhiApp getOptimizedSiddhiApp() {
        SiddhiApp localSiddhiApp = this.siddhiApp;

        for (ExecutionElement executionElement : siddhiApp.getExecutionElementList()) {
            if (executionElement instanceof Query && ((Query) executionElement).getInputStream() instanceof JoinInputStream) {
                executionElement = applyStreamJoinQueryOptimization((Query) executionElement);
            }

            if (executionElement instanceof Query && ((Query) executionElement).getInputStream() instanceof SingleInputStream) {
                executionElement = applySingleStreamOptimizations((Query)executionElement);
            }
        }

        return localSiddhiApp;
    }

    /**
     * This method concerns with the reordering of queries of the type SelectJoinProject(SJP) Queries.
     *
     * @param joinQuery (Query) Query of the type Select Join Project.
     * @return (Query) Query Object of the type Select-Join-Project which is re-ordered with Optimization rules.
     */
    private ExecutionElement applyStreamJoinQueryOptimization(Query joinQuery) {
        String leftInputStreamId = ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).getStreamId();
        String leftInputStreamRef = ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).getStreamReferenceId();
        String rightInputStreamId = ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getRightInputStream()).getStreamId();
        String rightInputStreamRef = ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getRightInputStream()).getStreamReferenceId();
        InputFilterModel leftInputStream = new InputFilterModel(leftInputStreamId, leftInputStreamRef, completeStreamMap.get(leftInputStreamId));
        InputFilterModel rightInputStream = new InputFilterModel(rightInputStreamId, rightInputStreamRef, completeStreamMap.get(rightInputStreamId));

        StreamJoinQueryModel joinQueryModel = new StreamJoinQueryModel(leftInputStream,
                rightInputStream, joinQuery.getSelector().getSelectionList());

        if (!((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).getStreamHandlers().isEmpty() &&
                ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).getStreamHandlers().get(0) instanceof Filter) {

            joinQueryModel.getLeftInputStream()
                    .setFilterExpression(((Filter) ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream())
                            .getLeftInputStream()).getStreamHandlers().get(0)).getFilterExpression());

        }

        if (!((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).getStreamHandlers().isEmpty() &&
                ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getRightInputStream()).getStreamHandlers().get(0) instanceof Filter) {

            joinQueryModel.getRightInputStream()
                    .setFilterExpression(((Filter) ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream())
                            .getRightInputStream()).getStreamHandlers().get(0)).getFilterExpression());

        }

        HashMap<String, Expression> onConditionReformatted =
                evaluateOnCondition(((JoinInputStream) joinQuery.getInputStream()).getOnCompare(),
                        leftInputStream, rightInputStream);

        // apply reorder optimization for onCompare expression of Join queries
        applyDepthFirstReorder(((JoinInputStream) joinQuery.getInputStream()).getOnCompare(), 0);

        // apply reorder optimization for filter conditions of the joining streams expression of Join queries
        if (leftInputStream.getFilterExpression() != null) {
//            Expression filterExp = ((Filter) ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).getStreamHandlers().get(0)).getFilterExpression();

            applyDepthFirstReorder(leftInputStream.getFilterExpression(), 0);

            if (!((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).getStreamHandlers().isEmpty() &&
                    ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).getStreamHandlers().get(0) instanceof Filter) {
                ((Filter) ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).getStreamHandlers().get(0)).setFilterExpression(leftInputStream.getFilterExpression());
            } else {
                ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).getStreamHandlers().add(0, new Filter(leftInputStream.getFilterExpression()));
                if (((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).getWindowPosition() > -1) {
                    ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).setWindowPosition(1);
                }
            }

        }

        if (rightInputStream.getFilterExpression() != null) {
//            Expression filterExp = ((Filter) ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getRightInputStream()).getStreamHandlers().get(0)).getFilterExpression();

            applyDepthFirstReorder(rightInputStream.getFilterExpression(), 0);

            if (!((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).getStreamHandlers().isEmpty() &&
                    ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getRightInputStream()).getStreamHandlers().get(0) instanceof Filter) {
                ((Filter) ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getRightInputStream()).getStreamHandlers().get(0)).setFilterExpression(rightInputStream.getFilterExpression());
            } else {
                Filter filter = new Filter(leftInputStream.getFilterExpression());
                ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getRightInputStream()).getStreamHandlers().add(0, filter);
                if (((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).getWindowPosition() > -1) {
                    ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).setWindowPosition(1);
                }
            }
        }

        // apply reordering for having condition in Join Query
        applyDepthFirstReorder(joinQuery.getSelector().getHavingExpression(), 0);


        return joinQuery;
    }

    private ExecutionElement applySingleStreamOptimizations(Query singleInputStreamQuery) {
        if (singleInputStreamQuery.getInputStream() instanceof BasicSingleInputStream) {
            if (!((BasicSingleInputStream) singleInputStreamQuery.getInputStream()).getStreamHandlers().isEmpty() &&
                    ((BasicSingleInputStream) singleInputStreamQuery.getInputStream()).getStreamHandlers().get(0) instanceof Filter) {
                applyDepthFirstReorder(((Filter) ((BasicSingleInputStream) singleInputStreamQuery.getInputStream()).getStreamHandlers().get(0)).getFilterExpression(), 0);
            }

            if (singleInputStreamQuery.getSelector().getHavingExpression() != null) {
                applyDepthFirstReorder(singleInputStreamQuery.getSelector().getHavingExpression(), 0);
            }
        }

        return singleInputStreamQuery;
    }

    /**
     * This method evaluates onCondition to determine which filters belong strictly to on Expression and which conditions
     * can be reordered.
     *
     * @param expression            The original Query onCondition
     * @param leftInputFilterModel  streamId of the leftInputStream of JoinQuery
     * @param rightInputFilterModel streamId of the rightInputStream of JoinQuery
     * @return
     */
    private HashMap<String, Expression> evaluateOnCondition(Expression expression,
                                                            InputFilterModel leftInputFilterModel,
                                                            InputFilterModel rightInputFilterModel) {

        HashMap<String, Expression> expressionHashMap = new HashMap<>();

        applyDepthFirstReorder(expression, 0);

        if (expression instanceof Or || expression instanceof Not) {// Or conditions cannot be reordered because the separation of them is a union and separation of Not is too complicated
            expressionHashMap.put("onCondition", expression);
        } else {
            applyOnConditionReorder(expression, leftInputFilterModel, rightInputFilterModel);


        }

        return expressionHashMap;
    }


    /**
     * This method splits the onCondition checking if the operations in the on conditions are really necessary and moves
     * them as a selection upwords in the query block.
     *
     * @param expression
     * @param lifModel
     * @param rifModel
     * @return
     */
    private boolean applyOnConditionReorder(Expression expression, InputFilterModel lifModel, InputFilterModel rifModel) {
        boolean left = false;
        boolean right = false;

        if (expression instanceof And) {
            left = applyOnConditionReorder(((And) expression).getLeftExpression(), lifModel, rifModel);
            right = applyOnConditionReorder(((And) expression).getRightExpression(), lifModel, rifModel);

            if (!left && !right) {
                moveSelection(((And) expression).getLeftExpression(), lifModel, rifModel);
                moveSelection(((And) expression).getRightExpression(), lifModel, rifModel);
                return false;
            } else if (!left) {
                moveSelection(((And) expression).getLeftExpression(), lifModel, rifModel);
                expression = ((And) expression).getRightExpression();
            } else if (!right) {
                moveSelection(((And) expression).getRightExpression(), lifModel, rifModel);
                expression = ((And) expression).getRightExpression();
            }

        } else if (expression instanceof Condition) {
            if (((Condition) expression).getRightExpression() instanceof Variable && ((Condition) expression).getLeftExpression() instanceof Variable)
                return true;
            if (((Condition) expression).getRightExpression() instanceof Operation && ((Condition) expression).getLeftExpression() instanceof Operation)
                return true;

            return (((Condition) expression).getLeftExpression() instanceof Variable || ((Condition) expression).getRightExpression() instanceof Variable) &&
                    (((Condition) expression).getRightExpression() instanceof Operation || ((Condition) expression).getLeftExpression() instanceof Operation);
        }

        return left || right;
    }


//    private void applyHavingConditionReorderForJoin(Expression expression, InputFilterModel lifModel, InputFilterModel rifModel) {
//
//    }

    private void moveSelection(Expression expression, InputFilterModel leftInputFilter, InputFilterModel rightInputFilter) {
        if (((Compare) expression).getLeftExpression() instanceof Variable || ((Compare) expression).getLeftExpression() instanceof Operation) {
            if (((Compare) expression).getLeftExpression() instanceof Variable) {
                if (((Variable) ((Compare) expression).getLeftExpression()).getStreamId().equals(leftInputFilter.getStreamId()) ||
                        ((Variable) ((Compare) expression).getLeftExpression()).getStreamId().equals(leftInputFilter.getStreamRefId())) {
                    ((Variable) ((Compare) expression).getLeftExpression()).setStreamId(null);
                    leftInputFilter.addConjunctiveFilterExpression(expression);
                } else {
                    ((Variable) ((Compare) expression).getLeftExpression()).setStreamId(null);
                    rightInputFilter.addConjunctiveFilterExpression(expression);
                }
            } else {
                if (((Variable) ((Operation) ((Compare) expression).getLeftExpression()).getLeftValue()).getStreamId().equals(leftInputFilter.getStreamId())
                        || ((Variable) ((Operation) ((Compare) expression).getLeftExpression()).getLeftValue()).getStreamId().equals(leftInputFilter.getStreamRefId())) {
                    ((Variable) ((Operation) ((Compare) expression).getLeftExpression()).getLeftValue()).setStreamId(null);
                    leftInputFilter.addConjunctiveFilterExpression(expression);
                } else {
                    ((Variable) ((Operation) ((Compare) expression).getLeftExpression()).getLeftValue()).setStreamId(null);
                    rightInputFilter.addConjunctiveFilterExpression(expression);
                }
            }
        } else {
            if (((Compare) expression).getRightExpression() instanceof Variable) {
                if (((Variable) ((Compare) expression).getRightExpression()).getStreamId().equals(leftInputFilter.getStreamId()) ||
                        ((Variable) ((Compare) expression).getRightExpression()).getStreamId().equals(leftInputFilter.getStreamRefId())) {
                    ((Variable) ((Compare) expression).getRightExpression()).setStreamId(leftInputFilter.getStreamId());
                    leftInputFilter.addConjunctiveFilterExpression(expression);
                } else {
                    ((Variable) ((Compare) expression).getRightExpression()).setStreamId(rightInputFilter.getStreamId());
                    rightInputFilter.addConjunctiveFilterExpression(expression);
                }
            } else {
                if (((Variable) ((Operation) ((Compare) expression).getRightExpression()).getLeftValue()).getStreamId().equals(leftInputFilter.getStreamId())
                        || ((Variable) ((Operation) ((Compare) expression).getRightExpression()).getLeftValue()).getStreamId().equals(leftInputFilter.getStreamRefId())) {
                    ((Variable) ((Operation) ((Compare) expression).getRightExpression()).getLeftValue()).setStreamId(leftInputFilter.getStreamId());
                    leftInputFilter.addConjunctiveFilterExpression(expression);
                } else {
                    ((Variable) ((Operation) ((Compare) expression).getRightExpression()).getLeftValue()).setStreamId(rightInputFilter.getStreamId());
                    rightInputFilter.addConjunctiveFilterExpression(expression);
                }
            }
        }
    }

    /**
     * This method reorders a given condition tree(Filter expression) giving leftHandExpression the lowest depth.
     *
     * @param expression Expression to be reordered
     * @param depth      depth of the branch
     * @return
     */
    private int applyDepthFirstReorder(Expression expression, int depth) {
        int leftDepth = 0;
        int rightDepth = 0;

        if (expression instanceof And || expression instanceof Or || expression instanceof Not) {
            if (expression instanceof And) {
                leftDepth = applyDepthFirstReorder(((And) expression).getLeftExpression(), depth + 1);
                rightDepth = applyDepthFirstReorder(((And) expression).getRightExpression(), depth + 1);

                if (leftDepth > rightDepth) {
                    Expression leftExpression = ((And) expression).getLeftExpression();
                    ((And) expression).setLeftExpression(((And) expression).getRightExpression());
                    ((And) expression).setRightExpression(leftExpression);
                }
            } else if (expression instanceof Or) {
                leftDepth = applyDepthFirstReorder(((Or) expression).getLeftExpression(), depth + 1);
                rightDepth = applyDepthFirstReorder(((Or) expression).getRightExpression(), depth + 1);

                if (leftDepth > rightDepth) {
                    Expression leftExpression = ((Or) expression).getLeftExpression();
                    ((Or) expression).setLeftExpression(((Or) expression).getRightExpression());
                    ((Or) expression).setRightExpression(leftExpression);
                }
            } else {
                return applyDepthFirstReorder(((Not) expression).getExpression(), depth + 1);
            }
        }

        return (leftDepth == 0 && rightDepth == 0 ? depth + 1 : (leftDepth > rightDepth ? leftDepth + 1 : rightDepth + 1));
    }

    /**
     * Method to extract all stream definitions and attributes for the purpose of filter remapping.
     */
    private void extractCompleteStreamMap(SiddhiApp siddhiApp) {
        // First extract data on Stream definition map
        for (StreamDefinition streamDefinition : siddhiApp.getStreamDefinitionMap().values()) {
            completeStreamMap.put(streamDefinition.getId(),
                    streamDefinition.getAttributeList().stream().map(Attribute::getName).collect(Collectors.toList()));
        }

        // Then extract the data of the temporary streams
        for (ExecutionElement executionElement : siddhiApp.getExecutionElementList()) {
            if (executionElement instanceof Query) { // there are other executionElementTypes(Ex: Partition)

                if (!completeStreamMap.containsKey(((Query) executionElement).getOutputStream().getId())) {
                    completeStreamMap.put(((Query) executionElement).getOutputStream().getId(),
                            ((Query) executionElement).getSelector().getSelectionList()
                                    .stream().map(OutputAttribute::getRename).collect(Collectors.toList()));
                }

            }
        }

    }
}
