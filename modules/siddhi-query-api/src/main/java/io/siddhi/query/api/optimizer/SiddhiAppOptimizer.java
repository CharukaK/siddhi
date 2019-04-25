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

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class SiddhiAppOptimizer {
    private HashMap<String, List<String>> completeStreamMap = new HashMap<>();
    private SiddhiApp siddhiApp;

    public SiddhiAppOptimizer(SiddhiApp siddhiApp) {
        this.siddhiApp = siddhiApp;
        extractCompleteStreamMap(siddhiApp);

        System.out.println("test");
    }

    public SiddhiApp getOptimizedSiddhiApp() {


        return siddhiApp;
    }

    private SiddhiApp applyFilterConditionsReorder(SiddhiApp siddhiApp) {
        for (ExecutionElement execElement: siddhiApp.getExecutionElementList()) {
            if(((Query)execElement).getInputStream() instanceof JoinInputStream) {

            } else if (((Query)execElement).getInputStream() instanceof SingleInputStream ||
                                            ((Query)execElement).getInputStream() instanceof BasicSingleInputStream) {
                if(((SingleInputStream)((Query)execElement).getInputStream())
                                                                .getStreamHandlers().get(0) instanceof Filter){

                }
            }
        }


        return siddhiApp;
    }

    /**
     * Method to extract all stream definitions and attributes for the purpose of filter remapping.
     */
    private void extractCompleteStreamMap(SiddhiApp siddhiApp) {
//        First extract data on Stream definition map
        for (StreamDefinition streamDefinition: siddhiApp.getStreamDefinitionMap().values()) {
            completeStreamMap.put(streamDefinition.getId(),
                    streamDefinition.getAttributeList().stream().map(Attribute::getName).collect(Collectors.toList()));
        }

        // Then extract the data of the temporary streams
        for (ExecutionElement executionElement: siddhiApp.getExecutionElementList()) {
            if (executionElement instanceof Query) { // there are other executionElementTypes(Ex: Partition)

                if(!completeStreamMap.containsKey(((Query)executionElement).getOutputStream().getId())) {
                    completeStreamMap.put(((Query)executionElement).getOutputStream().getId(),
                            ((Query)executionElement).getSelector().getSelectionList()
                                    .stream().map(OutputAttribute::getRename).collect(Collectors.toList()));
                }

            }
        }

    }
}
