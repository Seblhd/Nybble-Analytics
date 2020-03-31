package com.nybble.alpha.alert_engine;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;

public class MatchAggregation implements FlatMapFunction<Tuple2<ObjectNode, ObjectNode>, Tuple2<ObjectNode, ObjectNode>> {

    private HashMap<ObjectNode, Tuple2<Date, Long>> globalCountMap = new HashMap<>();
    private HashMap<ObjectNode, Tuple2<Date, Long>> fieldCountMap = new HashMap<>();
    private HashMap<ObjectNode, Tuple2<Date, Long>> fieldByGroupCountMap = new HashMap<>();
    private ObjectMapper jsonMapper = new ObjectMapper();
    private static TimeZone tz = TimeZone.getTimeZone("UTC");
    private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    public MatchAggregation() {
        // Set timezone for alert creation timestamp
        df.setTimeZone(tz);
    }

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

            if (aggregationNode.has("aggfield") && aggregationNode.has("groupfield")) {

                // Create fieldByGroupCountNode
                ObjectNode fieldByGroupCountNode = jsonMapper.createObjectNode();

                // Get value of aggfield from EventNode
                String aggfield = JsonPath.using(jsonPathConfig)
                        .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                        .read("$." + aggregationNode.get("aggfield").asText());

                // Get value of groupfield from EventNode
                String groupfield = JsonPath.using(jsonPathConfig)
                        .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                        .read("$." + aggregationNode.get("groupfield").asText());

                if (aggfield != null && groupfield != null) {
                    // Add values in fieldByGroupCountNode. This Node is the fieldByGroupCountMap HashMap key.
                    fieldByGroupCountNode.put("ruleid", controlEventMatch.f1.get("ruleid").asText());
                    fieldByGroupCountNode.put("aggfield", aggfield);
                    fieldByGroupCountNode.put("groupfield", groupfield);

                    System.out.println("Field by group count node is : " + fieldByGroupCountNode);

                    if (fieldByGroupCountMap.containsKey(fieldByGroupCountNode)) {

                    } else {
                        // Else, create Tuple2 with 1st event.created timestamp and count with value to 1.
                        Tuple2<Date, Long> aggregationTuple = new Tuple2<>();
                        aggregationTuple.f0 = df.parse(controlEventMatch.f0.get("event").get("created").asText());
                        aggregationTuple.f1 = 1L;

                        // Then create a new entry in HashMap with fieldByGroupCountNode as Key and aggregationTuple as value.
                        fieldByGroupCountMap.put(fieldByGroupCountNode, aggregationTuple);

                        System.out.println("Field by group count map is : " + fieldByGroupCountMap);
                    }

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
                        .read("$." + aggregationNode.get("aggfield").asText());

                if (aggfield != null) {
                    // Add values in fieldCountNode. This Node is the fieldCountMap HashMap key.
                    fieldCountNode.put("ruleid", controlEventMatch.f1.get("ruleid").asText());
                    fieldCountNode.put("aggfield", aggfield);

                    System.out.println("Field count node is : " + fieldCountNode);

                    if (fieldCountMap.containsKey(fieldCountNode)) {

                    } else {
                        // Else, create Tuple2 with 1st event.created timestamp and count with value to 1.
                        Tuple2<Date, Long> aggregationTuple = new Tuple2<>();
                        aggregationTuple.f0 = df.parse(controlEventMatch.f0.get("event").get("created").asText());
                        aggregationTuple.f1 = 1L;

                        // Then create a new entry in HashMap with fieldCountNode as Key and aggregationTuple as value.
                        fieldCountMap.put(fieldCountNode, aggregationTuple);
                    }

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

                if (globalCountMap.containsKey(globalCountNode)) {

                } else {
                    // Else, create Tuple2 with 1st event.created timestamp and count with value to 1.
                    Tuple2<Date, Long> aggregationTuple = new Tuple2<>();
                    aggregationTuple.f0 = df.parse(controlEventMatch.f0.get("event").get("created").asText());
                    aggregationTuple.f1 = 1L;

                    // Then create a new entry in HashMap with globalCountNode as Key and aggregationTuple as value.
                    globalCountMap.put(globalCountNode, aggregationTuple);
                }
            }
        }
    }
}
