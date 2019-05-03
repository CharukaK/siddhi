package io.siddhi.query.api.optimizer2;

import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.ExecutionElement;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.execution.query.selection.OutputAttribute;
import io.siddhi.query.api.optimizer2.beans.QueryModels.SPJQueryModel;

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
