package com.nybble.alpha.control_stream;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class MultipleRulesProcess extends ProcessFunction<ObjectNode, ObjectNode> {

    private ObjectMapper jsonMapper = new ObjectMapper();

    /*@Override
    public void open (Configuration parameters) throws Exception {

    }*/

    @Override
    public void processElement(ObjectNode controlNode, Context context, Collector<ObjectNode> collector) {

        if (controlNode.get("rule").size() > 1) {
            for (int x = 0; x < controlNode.get("rule").size(); x++) {
                // Create an new ObjectNode for each JsonRule in Control Event.
                ObjectNode singleRuleControl = jsonMapper.createObjectNode();
                ArrayNode singleRuleArray = jsonMapper.createArrayNode();

                // Create a new Array and a new node for each separate rules
                singleRuleArray.add(controlNode.get("rule").get(x));
                singleRuleControl.put("ruleid", controlNode.get("ruleid").asText());
                singleRuleControl.put("ruletitle", controlNode.get("ruletitle").asText());
                singleRuleControl.set("rule", singleRuleArray);

                // Collect each single rule
                collector.collect(singleRuleControl);
            }
        } else {
            // Else collect in case of single JsonPath rule.
            collector.collect(controlNode);
        }
    }
}
