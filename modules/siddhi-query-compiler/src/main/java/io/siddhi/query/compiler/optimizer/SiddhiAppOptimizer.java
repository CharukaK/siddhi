package io.siddhi.query.compiler.optimizer;

import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.ExecutionElement;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import io.siddhi.query.compiler.optimizer.beans.QueryModels.SPJQueryModel;
import io.siddhi.query.compiler.optimizer.beans.QueryModels.SPQueryModel;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class SiddhiAppOptimizer {
    private SiddhiApp siddhiApp;
    private HashMap<String, List<Attribute>> completeStreamMap = new HashMap<>();


    public SiddhiAppOptimizer(SiddhiApp siddhiApp) {
        this.siddhiApp = siddhiApp;
        extractCompleteStreamMap(siddhiApp);
    }

    public SiddhiApp getOptimizedApp() {

        for (ExecutionElement executionElement: siddhiApp.getExecutionElementList()) {
            if(executionElement instanceof Query && ((Query) executionElement).getInputStream() instanceof JoinInputStream) {
                SPJQueryModel spjQueryModel = new SPJQueryModel((Query) executionElement, this.completeStreamMap);
                spjQueryModel.applyOptimization();

                ((SingleInputStream)((JoinInputStream) ((Query) executionElement).getInputStream()).getLeftInputStream())
                        .setWindowPosition(spjQueryModel.getLeftInputModel().getWindowPosition());

                ((SingleInputStream)((JoinInputStream) ((Query) executionElement).getInputStream()).getRightInputStream())
                        .setWindowPosition(spjQueryModel.getRightInputModel().getWindowPosition());

                ((SingleInputStream)((JoinInputStream) ((Query) executionElement).getInputStream()).getLeftInputStream())
                        .setStreamHandlers(spjQueryModel.getLeftInputModel().getStreamFilters());

                ((SingleInputStream)((JoinInputStream) ((Query) executionElement).getInputStream()).getRightInputStream())
                        .setStreamHandlers(spjQueryModel.getRightInputModel().getStreamFilters());

                ((Query)executionElement).getSelector().setHavingExpression(spjQueryModel.getHavingCondition());
                ((JoinInputStream)((Query) executionElement).getInputStream()).setOnCompare(spjQueryModel.getOnConditionExpression());
            }

            if(executionElement instanceof Query && ((Query) executionElement).getInputStream() instanceof SingleInputStream) {
                SPQueryModel spQueryModel = new SPQueryModel((Query) executionElement, this.completeStreamMap);
                spQueryModel.applyOptimization();

                ((SingleInputStream) ((Query) executionElement).getInputStream()).setWindowPosition(spQueryModel.getInputModel().getWindowPosition());
                ((SingleInputStream) ((Query) executionElement).getInputStream()).setStreamHandlers(spQueryModel.getInputModel().getStreamFilters());
                ((Query) executionElement).getSelector().setHavingExpression(spQueryModel.getHavingCondition());
            }
        }

        return siddhiApp;
    }

    /**
     * Method to extract all stream definitions and attributes for the purpose of filter remapping.
     */
    private void extractCompleteStreamMap(SiddhiApp siddhiApp) {
        // First extract data on Stream definition map
        for(StreamDefinition streamDefinition: siddhiApp.getStreamDefinitionMap().values()) {
            completeStreamMap.put(streamDefinition.getId(), streamDefinition.getAttributeList());
        }

        // Then extract the table definition definitions
        for(TableDefinition tableDefinition: siddhiApp.getTableDefinitionMap().values()) {
            completeStreamMap.put(tableDefinition.getId(), tableDefinition.getAttributeList());
        }

        // Then extract the data of the temporary streams
        for (ExecutionElement executionElement: siddhiApp.getExecutionElementList()) {
            if(executionElement instanceof Query) {
                if(!completeStreamMap.containsKey(((Query) executionElement).getOutputStream().getId())) {
                    completeStreamMap.put(((Query) executionElement).getOutputStream().getId(),
                            ((Query) executionElement).getSelector()
                                    .getSelectionList().stream().map(element -> new Attribute("", null))
                                    .collect(Collectors.toList()));
                }
            }
        }



    }
}
