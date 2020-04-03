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

public class AverageFunction {

    // Create JsonPath configuration to search value of fields in EventNodes.
    private static HashMap<ObjectNode, Tuple2<Date, List<Long>>> fieldAvgMap = new HashMap<>();
    private static HashMap<ObjectNode, Tuple2<Date, List<Long>>> fieldByGroupAvgMap = new HashMap<>();
    private ObjectMapper jsonMapper = new ObjectMapper();
    private static TimeZone tz = TimeZone.getTimeZone("UTC");
    private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private Configuration jsonPathConfig = Configuration.defaultConfiguration()
            .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
            .addOptions(Option.SUPPRESS_EXCEPTIONS);

    public AverageFunction() {
        // Set timezone for alert creation timestamp
        df.setTimeZone(tz);
    }

    public boolean averageValue(Tuple2<ObjectNode, ObjectNode> controlEventMatch) throws JsonProcessingException, ParseException {

        boolean collectEvent = false;

        // Get aggregation node from rule
        ObjectNode aggregationNode = controlEventMatch.f1.get("rule").get(0).get("aggregation").deepCopy();

        if (aggregationNode.has("aggfield") && aggregationNode.has("groupfield")) {

            // Create fieldByGroupCountNode
            ObjectNode fieldByAvgCountNode = jsonMapper.createObjectNode();

            // Get value of groupfield from EventNode
            String groupfield = JsonPath.using(jsonPathConfig)
                    .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                    .read("$." + aggregationNode.get("groupfield").asText());

            if (groupfield != null) {
                // Add values in fieldByGroupCountNode. This Node is the fieldByGroupCountMap HashMap key.
                fieldByAvgCountNode.put("ruleid", controlEventMatch.f1.get("ruleid").asText());
                fieldByAvgCountNode.put("groupfield", groupfield);

                // If key already exists in fieldByGroupCountMap
                if (fieldByGroupAvgMap.containsKey(fieldByAvgCountNode)) {

                    // Get event.created Date of current event.
                    Date currentEventDate = df.parse(controlEventMatch.f0.get("event").get("created").asText());
                    // Get event.created date of 1st event added in Map.
                    Date firstEventDate = fieldByGroupAvgMap.get(fieldByAvgCountNode).f0;

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
                            try {
                                // If event value is highest, replace lowest value by highest value from matched event
                                fieldByGroupAvgMap.get(fieldByAvgCountNode).f1.add(Long.parseLong(aggfield));

                            } catch (NumberFormatException nb) {
                                System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() + "\" value is not a number. Max function can only be apply on number values.");
                            }
                        } else {
                            System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                                    "\" has not been found in event. Please check rule with id : " +
                                    controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
                        }

                        // Get average value by doing sum of all value in List<Long> in tuple and divide by List<Long> size.
                        Long fieldByGroupAvg = fieldByGroupAvgMap.get(fieldByAvgCountNode).f1.stream().mapToLong(Long::longValue).sum() / (long) fieldByGroupAvgMap.get(fieldByAvgCountNode).f1.size();

                        //Check if aggregation condition has been met.
                        boolean conditionFlag = new AssertAggregationCondition().conditionResult(aggregationNode.get("aggoperator").asText(),
                                fieldByGroupAvg,
                                aggregationNode.get("aggvalue").asLong());

                        // If still in Time gap and aggregation condition is met, collect event to create alert and remove entry in Map
                        if (conditionFlag) {
                            collectEvent = true;
                            fieldByGroupAvgMap.remove(fieldByAvgCountNode);
                        }
                    } else {
                        fieldByGroupAvgMap.remove(fieldByAvgCountNode);
                    }
                } else {
                    // Else, create Tuple2 with 1st event.created timestamp and count with value to 1.
                    Tuple2<Date, List<Long>> aggregationTuple = new Tuple2<>();
                    aggregationTuple.f0 = df.parse(controlEventMatch.f0.get("event").get("created").asText());

                    // Get aggfield value from EventNode
                    String aggfield = JsonPath.using(jsonPathConfig)
                            .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                            .read("$." + aggregationNode.get("aggfield").asText());

                    if (aggfield != null) {
                        aggregationTuple.f1 = new ArrayList<>();
                        try {
                            aggregationTuple.f1.add(Long.parseLong(aggfield));
                            // Then create a new entry in HashMap with fieldCountNode as Key and aggregationTuple as value.
                            fieldByGroupAvgMap.put(fieldByAvgCountNode, aggregationTuple);
                        } catch (NumberFormatException nb) {
                            System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() + "\" value is not a number. Max function can only be apply on number values.");
                        }
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

            // Create fieldAvgNode
            ObjectNode fieldAvgNode = jsonMapper.createObjectNode();

            // Add values in fieldAvgNode. This Node is the fieldAvgNode HashMap key.
            fieldAvgNode.put("ruleid", controlEventMatch.f1.get("ruleid").asText());

            // If key already exists in fieldAvgMap
            if (fieldAvgMap.containsKey(fieldAvgNode)) {

                // Get event.created Date of current event.
                Date currentEventDate = df.parse(controlEventMatch.f0.get("event").get("created").asText());
                // Get event.created date of 1st event added in Map.
                Date firstEventDate = fieldAvgMap.get(fieldAvgNode).f0;

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
                        try {
                            // If event value is highest, replace lowest value by highest value from matched event
                            fieldAvgMap.get(fieldAvgNode).f1.add(Long.parseLong(aggfield));

                        } catch (NumberFormatException nb) {
                            System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() + "\" value is not a number. Max function can only be apply on number values.");
                        }
                    } else {
                        System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                                "\" has not been found in event. Please check rule with id : " +
                                controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
                    }

                    // Get average value by doing sum of all value in List<Long> in tuple and divide by List<Long> size.
                    Long fieldByGroupAvg = fieldAvgMap.get(fieldAvgNode).f1.stream().mapToLong(Long::longValue).sum() / (long) fieldAvgMap.get(fieldAvgNode).f1.size();

                    //Check if aggregation condition has been met.
                    boolean conditionFlag = new AssertAggregationCondition().conditionResult(aggregationNode.get("aggoperator").asText(),
                            fieldByGroupAvg,
                            aggregationNode.get("aggvalue").asLong());

                    // If still in Time gap and aggregation condition is met, collect event to create alert and remove entry in Map
                    if (conditionFlag) {
                        collectEvent = true;
                        fieldAvgMap.remove(fieldAvgNode);
                    }
                } else {
                    fieldAvgMap.remove(fieldAvgNode);
                }
            } else {
                // Else, create Tuple2 with 1st event.created timestamp and count with value to 1.
                Tuple2<Date, List<Long>> aggregationTuple = new Tuple2<>();
                aggregationTuple.f0 = df.parse(controlEventMatch.f0.get("event").get("created").asText());

                // Get aggfield value from EventNode
                String aggfield = JsonPath.using(jsonPathConfig)
                        .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                        .read("$." + aggregationNode.get("aggfield").asText());

                if (aggfield != null) {
                    aggregationTuple.f1 = new ArrayList<>();
                    try {
                        aggregationTuple.f1.add(Long.parseLong(aggfield));
                        // Then create a new entry in HashMap with fieldCountNode as Key and aggregationTuple as value.
                        fieldAvgMap.put(fieldAvgNode, aggregationTuple);
                    } catch (NumberFormatException nb) {
                        System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() + "\" value is not a number. Max function can only be apply on number values.");
                    }
                } else {
                    System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                            "\" has not been found in event. Please check rule with id : " +
                            controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
                }
            }
        } else {
            System.out.println("Average aggregation function need at least \"aggfield\" to be set.");
        }

        return collectEvent;
    }
}
