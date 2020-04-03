package com.nybble.alpha.alert_engine.aggregation_functions;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.nybble.alpha.alert_engine.aggregation_functions.commons.AssertAggregationCondition;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class UniqueCountFunction {

    // Create JsonPath configuration to search value of fields in EventNodes.
    private static HashMap<ObjectNode, Tuple2<Date, List<String>>> fieldUniqueCountMap = new HashMap<>();
    private static HashMap<ObjectNode, Tuple2<Date, List<String>>> fieldByGroupUniqueCountMap = new HashMap<>();
    private ObjectMapper jsonMapper = new ObjectMapper();
    private static TimeZone tz = TimeZone.getTimeZone("UTC");
    private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private Configuration jsonPathConfig = Configuration.defaultConfiguration()
            .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
            .addOptions(Option.SUPPRESS_EXCEPTIONS);

    public UniqueCountFunction() {
        // Set timezone for alert creation timestamp
        df.setTimeZone(tz);
    }

    public boolean countEvent(Tuple2<ObjectNode, ObjectNode> controlEventMatch) throws JsonProcessingException, ParseException {

        boolean collectEvent = false;

        // Get aggregation node from rule
        ObjectNode aggregationNode = controlEventMatch.f1.get("rule").get(0).get("aggregation").deepCopy();

        if (aggregationNode.has("aggfield") && aggregationNode.has("groupfield")) {

            // Create fieldByGroupUniqueCountNode
            ObjectNode fieldByGroupUniqueCountNode = jsonMapper.createObjectNode();

            // Get value of groupfield from EventNode
            String groupfield = JsonPath.using(jsonPathConfig)
                    .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                    .read("$." + aggregationNode.get("groupfield").asText());

            if (groupfield != null) {
                // Add values in fieldByGroupUniqueCountNode. This Node is the fieldByGroupUniqueCountMap HashMap key.
                fieldByGroupUniqueCountNode.put("ruleid", controlEventMatch.f1.get("ruleid").asText());
                fieldByGroupUniqueCountNode.put("groupfield", groupfield);

                // If key already exists in fieldByGroupUniqueCountMap
                if (fieldByGroupUniqueCountMap.containsKey(fieldByGroupUniqueCountNode)) {

                    // Get event.created Date of current event.
                    Date currentEventDate = df.parse(controlEventMatch.f0.get("event").get("created").asText());
                    // Get event.created date of 1st event added in Map.
                    Date firstEventDate = fieldByGroupUniqueCountMap.get(fieldByGroupUniqueCountNode).f0;

                    // Get the number of seconds between firstEvent in Map and Current event time.
                    long timeGapSec = TimeUnit.MILLISECONDS.toSeconds(currentEventDate.getTime() - firstEventDate.getTime());

                    // Check if events is still in aggregation Timeframe.
                    // If Time gap is inferior to Timefram, then increment count by one and then check operator and value number.
                    // Else, delete entry in Map for this aggregation
                    if (timeGapSec < controlEventMatch.f1.get("rule").get(0).get("timeframe").get("duration").asLong()) {

                        // Check if aggfield value already exists in List in Tuple2. If yes, do nothing, unique value is already there.
                        // If not, add aggfield value in List in Tuple2.

                        // Get aggfield value from EventNode
                        String aggfield = JsonPath.using(jsonPathConfig)
                                .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                                .read("$." + aggregationNode.get("aggfield").asText());

                        if (aggfield != null) {
                            if (!fieldByGroupUniqueCountMap.get(fieldByGroupUniqueCountNode).f1.contains(aggfield)) {
                                fieldByGroupUniqueCountMap.get(fieldByGroupUniqueCountNode).f1.add(aggfield);
                            }
                        } else {
                            System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                                    "\" has not been found in event. Please check rule with id : " +
                                    controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
                        }

                        //Check if aggregation condition has been met.
                        boolean conditionFlag = new AssertAggregationCondition().conditionResult(aggregationNode.get("aggoperator").asText(),
                                Integer.toUnsignedLong(fieldByGroupUniqueCountMap.get(fieldByGroupUniqueCountNode).f1.size()),
                                aggregationNode.get("aggvalue").asLong());

                        // If still in Time gap and aggregation condition is met, collect event to create alert and remove entry in Map
                        if (conditionFlag) {
                            collectEvent = true;
                            fieldByGroupUniqueCountMap.remove(fieldByGroupUniqueCountNode);
                        }
                    } else {
                        fieldByGroupUniqueCountMap.remove(fieldByGroupUniqueCountNode);
                    }
                } else {
                    // Else, create Tuple2 with 1st event.created timestamp and count with value to 1.
                    Tuple2<Date, List<String>> aggregationTuple = new Tuple2<>();
                    aggregationTuple.f0 = df.parse(controlEventMatch.f0.get("event").get("created").asText());

                    // Get aggfield value from EventNode
                    String aggfield = JsonPath.using(jsonPathConfig)
                            .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                            .read("$." + aggregationNode.get("aggfield").asText());

                    if (aggfield != null) {
                        aggregationTuple.f1 = new ArrayList<>();
                        aggregationTuple.f1.add(aggfield);
                        // Then create a new entry in HashMap with fieldByGroupUniqueCountNode as Key and aggregationTuple as value.
                        fieldByGroupUniqueCountMap.put(fieldByGroupUniqueCountNode, aggregationTuple);
                    } else {
                        System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                                "\" has not been found in event. Please check rule with id : " +
                                controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
                    }
                }
            } else {
                System.out.println("\"group-field\":\"" + aggregationNode.get("groupfield").asText() +
                        "\" has not been found in event. Please check rule with id : " +
                        controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
            }
        } else if (aggregationNode.has("aggfield") && !aggregationNode.has("groupfield")) {

            // Create fieldUniqueCountNode
            ObjectNode fieldUniqueCountNode = jsonMapper.createObjectNode();

            // Add values in fieldUniqueCountNode. This Node is the fieldUniqueCountMap HashMap key.
            fieldUniqueCountNode.put("ruleid", controlEventMatch.f1.get("ruleid").asText());

            // If key already exists in fieldUniqueCountMap
            if (fieldUniqueCountMap.containsKey(fieldUniqueCountNode)) {

                // Get event.created Date of current event.
                Date currentEventDate = df.parse(controlEventMatch.f0.get("event").get("created").asText());
                // Get event.created date of 1st event added in Map.
                Date firstEventDate = fieldUniqueCountMap.get(fieldUniqueCountNode).f0;

                // Get the number of seconds between firstEvent in Map and Current event time.
                long timeGapSec = TimeUnit.MILLISECONDS.toSeconds(currentEventDate.getTime() - firstEventDate.getTime());

                // Check if events is still in aggregation Timeframe.
                // If Time gap is inferior to Timefram, then increment count by one and then check operator and value number.
                // Else, delete entry in Map for this aggregation
                if (timeGapSec < controlEventMatch.f1.get("rule").get(0).get("timeframe").get("duration").asLong()) {

                    // Check if aggfield value already exists in List in Tuple2. If yes, do nothing, unique value is already there.
                    // If not, add aggfield value in List in Tuple2.

                    // Get aggfield value from EventNode
                    String aggfield = JsonPath.using(jsonPathConfig)
                            .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                            .read("$." + aggregationNode.get("aggfield").asText());

                    if (aggfield != null) {
                        if (!fieldUniqueCountMap.get(fieldUniqueCountNode).f1.contains(aggfield)) {
                            fieldUniqueCountMap.get(fieldUniqueCountNode).f1.add(aggfield);
                        }
                    } else {
                        System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                                "\" has not been found in event. Please check rule with id : " +
                                controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
                    }

                    //Check if aggregation condition has been met.
                    boolean conditionFlag = new AssertAggregationCondition().conditionResult(aggregationNode.get("aggoperator").asText(),
                            Integer.toUnsignedLong(fieldUniqueCountMap.get(fieldUniqueCountNode).f1.size()),
                            aggregationNode.get("aggvalue").asLong());

                    // If still in Time gap and aggregation condition is met, collect event to create alert and remove entry in Map
                    if (conditionFlag) {
                        collectEvent = true;
                        fieldUniqueCountMap.remove(fieldUniqueCountNode);
                    }
                } else {
                    fieldUniqueCountMap.remove(fieldUniqueCountNode);
                }
            } else {
                // Else, create Tuple2 with 1st event.created timestamp and count with value to 1.
                Tuple2<Date, List<String>> aggregationTuple = new Tuple2<>();
                aggregationTuple.f0 = df.parse(controlEventMatch.f0.get("event").get("created").asText());

                // Get aggfield value from EventNode
                String aggfield = JsonPath.using(jsonPathConfig)
                        .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                        .read("$." + aggregationNode.get("aggfield").asText());

                if (aggfield != null) {
                    aggregationTuple.f1.add(aggfield);
                    // Then create a new entry in HashMap with fieldUniqueCountNode as Key and aggregationTuple as value.
                    fieldUniqueCountMap.put(fieldUniqueCountNode, aggregationTuple);
                } else {
                    System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                            "\" has not been found in event. Please check rule with id : " +
                            controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
                }
            }
        } else {
            System.out.println("Unique count aggregation function need at least \"aggfield\" to be set.");
        }

        return collectEvent;
    }
}
