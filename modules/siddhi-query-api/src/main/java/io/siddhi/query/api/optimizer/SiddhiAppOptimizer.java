package io.siddhi.query.api.optimizer;

import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.execution.ExecutionElement;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.handler.Filter;
import io.siddhi.query.api.execution.query.input.stream.BasicSingleInputStream;
import io.siddhi.query.api.execution.query.input.stream.InputStream;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import io.siddhi.query.api.execution.query.selection.OutputAttribute;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.condition.And;
import io.siddhi.query.api.expression.condition.Not;
import io.siddhi.query.api.expression.condition.Or;
import io.siddhi.query.api.optimizer.beans.InputFilterBean;
import io.siddhi.query.api.optimizer.beans.StreamJoinQueryBean;

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
        String rightInputStreamId = ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getRightInputStream()).getStreamId();
        InputFilterBean leftInputStream = new InputFilterBean(leftInputStreamId, completeStreamMap.get(leftInputStreamId));
        InputFilterBean rightInputStream = new InputFilterBean(rightInputStreamId, completeStreamMap.get(rightInputStreamId));

        StreamJoinQueryBean joinQueryBean = new StreamJoinQueryBean(leftInputStream, rightInputStream, joinQuery.getSelector().getSelectionList());

        applyDepthFirstReorder(((JoinInputStream) joinQuery.getInputStream()).getOnCompare(), 0);

        if (((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).getStreamHandlers().get(0) instanceof Filter) {
            Expression filterExp = ((Filter) ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).getStreamHandlers().get(0)).getFilterExpression();

            applyDepthFirstReorder(filterExp, 0);

            ((Filter) ((SingleInputStream) ((JoinInputStream) joinQuery.getInputStream()).getLeftInputStream()).getStreamHandlers().get(0)).setFilterExpression(filterExp);

            System.out.println("Test");
        }

        applyDepthFirstReorder(joinQuery.getSelector().getHavingExpression(), 0);

        HashMap<String, Expression> onConditionReformatted = evaluateOnCondition(((JoinInputStream) joinQuery.getInputStream()).getOnCompare(), leftInputStreamId, rightInputStreamId);

        return joinQuery;
    }

    private HashMap<String, Expression> evaluateOnCondition(Expression expression, String leftStreamId, String rightStreamId) {
        HashMap<String, Expression> expressionHashMap = new HashMap<>();

        applyDepthFirstReorder(expression, 0);

        if (expression instanceof Or) {// Or conditions cannot be reordered because the separation of them is a union

        }

        return null;
    }

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
