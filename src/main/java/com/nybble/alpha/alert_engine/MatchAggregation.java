package com.nybble.alpha.alert_engine;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.HashMap;

public class MatchAggregation implements FlatMapFunction<Tuple2<ObjectNode, ObjectNode>, Tuple2<ObjectNode, ObjectNode>> {

    private HashMap<ObjectNode, Tuple2<Date, Integer>> globalCountMap;
    private HashMap<ObjectNode, Tuple2<Date, Integer>> fieldCountMap;
    private HashMap<ObjectNode, Tuple2<Date, Integer>> fieldByGroupCountMap;
    private ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    public void flatMap(Tuple2<ObjectNode, ObjectNode> controlEventMatch, Collector<Tuple2<ObjectNode, ObjectNode>> collector) throws Exception {

        // controlEventMatch.f0 is Event Node
        // controlEventMatch.f1 is ControlNode

        // If aggregation and timeframe are empty, then collect events.
        if (controlEventMatch.f1.get("rule").get(0).get("aggregation").isNull()
                && controlEventMatch.f1.get("rule").get(0).get("timeframe").isNull()) {
            collector.collect(Tuple2.of(controlEventMatch.f0, controlEventMatch.f1));
        } else if (!controlEventMatch.f1.get("rule").get(0).get("aggregation").isNull()
                && !controlEventMatch.f1.get("rule").get(0).get("timeframe").isNull()) {


            // Create JsonPath configuration to search value of fields in EventNodes.
            Configuration jsonPathConfig = Configuration.defaultConfiguration()
                    .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
                    .addOptions(Option.SUPPRESS_EXCEPTIONS);

            // Get aggregation node from rule
            ObjectNode aggregationNode = controlEventMatch.f1.get("rule").get(0).get("aggregation").deepCopy();

            System.out.println("Aggregation node is : " + aggregationNode.toString());

            if (aggregationNode.has("aggfield") && aggregationNode.has("groupfield")) {

                // Create fieldByGroupCountNode
                ObjectNode fieldByGroupCountNode = jsonMapper.createObjectNode();

                // Get value of aggfield from EventNode
                String aggfield = JsonPath.using(jsonPathConfig)
                        .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                        .read("$." + aggregationNode.get("aggfield"));

                // Get value of groupfield from EventNode
                String groupfield = JsonPath.using(jsonPathConfig)
                        .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                        .read("$." + aggregationNode.get("groupfield"));

                if (aggfield != null && groupfield != null) {
                    // Add values in fieldByGroupCountNode. This Node is the fieldByGroupCountMap HashMap key.
                    fieldByGroupCountNode.put("ruleid", controlEventMatch.f1.get("ruleid").asText());
                    fieldByGroupCountNode.put("aggfield", aggfield);
                    fieldByGroupCountNode.put("groupfield", groupfield);

                    System.out.println("Field by group count node is : " + fieldByGroupCountNode);

                } else {
                    System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                            "\" or \"group-field\":\"" + aggregationNode.get("groupfield").asText() +
                            "\" has not been found in event. Please check rule with id : " +
                            controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
                }

            } else if (aggregationNode.has("aggfield") && !aggregationNode.has("groupfield")) {

                // Create fieldCountNode
                ObjectNode fieldCountNode = jsonMapper.createObjectNode();

                // Get value of aggfield from EventNode
                String aggfield = JsonPath.using(jsonPathConfig)
                        .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                        .read("$." + aggregationNode.get("aggfield"));

                if (aggfield != null) {
                    // Add values in fieldCountNode. This Node is the fieldCountMap HashMap key.
                    fieldCountNode.put("ruleid", controlEventMatch.f1.get("ruleid").asText());
                    fieldCountNode.put("aggfield", aggfield);

                    System.out.println("Field  count node is : " + fieldCountNode);

                } else {
                    System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                            "\" has not been found in event. Please check rule with id : " +
                            controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
                }

            } else if (!aggregationNode.has("aggfield") && !aggregationNode.has("groupfield")) {

                // Create globalCountNode
                ObjectNode globalCountNode = jsonMapper.createObjectNode();

                // Add values in globalCountNode. This Node is the globalCountMap HashMap key.
                globalCountNode.put("ruleid", controlEventMatch.f1.get("ruleid").asText());

                System.out.println("Global count node is : " + globalCountNode);
            }
        }
    }
}
