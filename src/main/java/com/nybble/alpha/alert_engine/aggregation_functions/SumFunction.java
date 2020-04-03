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
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class SumFunction {

    // Create JsonPath configuration to search value of fields in EventNodes.
    private static HashMap<ObjectNode, Tuple2<Date, Long>> fieldSumMap = new HashMap<>();
    private static HashMap<ObjectNode, Tuple2<Date, Long>> fieldByGroupSumMap = new HashMap<>();
    private ObjectMapper jsonMapper = new ObjectMapper();
    private static TimeZone tz = TimeZone.getTimeZone("UTC");
    private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private Configuration jsonPathConfig = Configuration.defaultConfiguration()
            .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
            .addOptions(Option.SUPPRESS_EXCEPTIONS);

    public SumFunction() {
        // Set timezone for alert creation timestamp
        df.setTimeZone(tz);
    }

    public boolean sumValues(Tuple2<ObjectNode, ObjectNode> controlEventMatch) throws JsonProcessingException, ParseException {

        boolean collectEvent = false;

        // Get aggregation node from rule
        ObjectNode aggregationNode = controlEventMatch.f1.get("rule").get(0).get("aggregation").deepCopy();

        if (aggregationNode.has("aggfield") && aggregationNode.has("groupfield")) {

            // Create fieldByGroupSumNode
            ObjectNode fieldByGroupSumNode = jsonMapper.createObjectNode();

            // Get value of groupfield from EventNode
            String groupfield = JsonPath.using(jsonPathConfig)
                    .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                    .read("$." + aggregationNode.get("groupfield").asText());

            if (groupfield != null) {

                // Add values in fieldByGroupSumNode. This Node is the fieldByGroupSumMap HashMap key.
                fieldByGroupSumNode.put("ruleid", controlEventMatch.f1.get("ruleid").asText());
                fieldByGroupSumNode.put("groupfield", groupfield);

                // If key already exists in fieldByGroupSumMap
                if (fieldByGroupSumMap.containsKey(fieldByGroupSumNode)) {

                    // Get event.created Date of current event.
                    Date currentEventDate = df.parse(controlEventMatch.f0.get("event").get("created").asText());
                    // Get event.created date of 1st event added in Map.
                    Date firstEventDate = fieldByGroupSumMap.get(fieldByGroupSumNode).f0;

                    // Get the number of seconds between firstEvent in Map and Current event time.
                    long timeGapSec = TimeUnit.MILLISECONDS.toSeconds(currentEventDate.getTime() - firstEventDate.getTime());

                    // Check if events is still in aggregation Timeframe.
                    // If Time gap is inferior to Timefram, then increment count by one and then check operator and value number.
                    // Else, delete entry in Map for this aggregation
                    if (timeGapSec < controlEventMatch.f1.get("rule").get(0).get("timeframe").get("duration").asLong()) {

                        // Get aggfield value from EventNode
                        String aggfield = JsonPath.using(jsonPathConfig)
                                .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                                .read("$." + aggregationNode.get("aggfield").asText());

                        if (aggfield != null) {
                            try {
                                // Add value from event to total value in Tuple2.
                                fieldByGroupSumMap.get(fieldByGroupSumNode).f1 = fieldByGroupSumMap.get(fieldByGroupSumNode).f1 + Long.parseLong(aggfield);
                            } catch (NumberFormatException nb) {
                                System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() + "\" value is not a number. Sum function can only be apply on number values.");
                            }
                        } else {
                            System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                                    "\" has not been found in event. Please check rule with id : " +
                                    controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
                        }

                        //Check if aggregation condition has been met.
                        boolean conditionFlag = new AssertAggregationCondition().conditionResult(aggregationNode.get("aggoperator").asText(),
                                fieldByGroupSumMap.get(fieldByGroupSumNode).f1,
                                aggregationNode.get("aggvalue").asLong());

                        // If still in Time gap and aggregation condition is met, collect event to create alert and remove entry in Map
                        if (conditionFlag) {
                            collectEvent = true;
                            fieldByGroupSumMap.remove(fieldByGroupSumNode);
                        }
                    } else {
                        fieldByGroupSumMap.remove(fieldByGroupSumNode);
                    }
                } else {
                    // Else, create Tuple2 with 1st event.created timestamp and aggfield value from 1st event.
                    Tuple2<Date, Long> aggregationTuple = new Tuple2<>();
                    aggregationTuple.f0 = df.parse(controlEventMatch.f0.get("event").get("created").asText());

                    // Get aggfield value from EventNode
                    String aggfield = JsonPath.using(jsonPathConfig)
                            .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                            .read("$." + aggregationNode.get("aggfield").asText());

                    if (aggfield != null) {
                        try {
                            aggregationTuple.f1 = Long.parseLong(aggfield);
                            // Then create a new entry in HashMap with fieldByGroupSumNode as Key and aggregationTuple as value.
                            fieldByGroupSumMap.put(fieldByGroupSumNode, aggregationTuple);
                        } catch (NumberFormatException nb) {
                            System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() + "\" value is not a number. Sum function can only be apply on number values.");
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

            // Create fieldByGroupSumNode
            ObjectNode fieldSumNode = jsonMapper.createObjectNode();

            // Add values in fieldSumNode. This Node is the fieldSumMap HashMap key.
            fieldSumNode.put("ruleid", controlEventMatch.f1.get("ruleid").asText());

            // If key already exists in fieldSumMap
            if (fieldSumMap.containsKey(fieldSumNode)) {
                // Get event.created Date of current event.
                Date currentEventDate = df.parse(controlEventMatch.f0.get("event").get("created").asText());
                // Get event.created date of 1st event added in Map.
                Date firstEventDate = fieldSumMap.get(fieldSumNode).f0;

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
                            // Add value from event to total value in Tuple2.
                            fieldSumMap.get(fieldSumNode).f1 = fieldSumMap.get(fieldSumNode).f1 + Long.parseLong(aggfield);
                        } catch (NumberFormatException nb) {
                            System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() + "\" value is not a number. Sum function can only be apply on number values.");
                        }
                    } else {
                        System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                                "\" has not been found in event. Please check rule with id : " +
                                controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
                    }

                    //Check if aggregation condition has been met.
                    boolean conditionFlag = new AssertAggregationCondition().conditionResult(aggregationNode.get("aggoperator").asText(),
                            fieldSumMap.get(fieldSumNode).f1,
                            aggregationNode.get("aggvalue").asLong());

                    // If still in Time gap and aggregation condition is met, collect event to create alert and remove entry in Map
                    if (conditionFlag) {
                        collectEvent = true;
                        fieldSumMap.remove(fieldSumNode);
                    }
                } else {
                    fieldSumMap.remove(fieldSumNode);
                }
            } else {
                // Else, create Tuple2 with 1st event.created timestamp and count with value to 1.
                Tuple2<Date, Long> aggregationTuple = new Tuple2<>();
                aggregationTuple.f0 = df.parse(controlEventMatch.f0.get("event").get("created").asText());

                // Get aggfield value from EventNode
                String aggfield = JsonPath.using(jsonPathConfig)
                        .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                        .read("$." + aggregationNode.get("aggfield").asText());

                if (aggfield != null) {
                    try {
                        aggregationTuple.f1 = Long.parseLong(aggfield);
                        // Then create a new entry in HashMap with fieldSumNode as Key and aggregationTuple as value.
                        fieldSumMap.put(fieldSumNode, aggregationTuple);
                    } catch (NumberFormatException nb) {
                        System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() + "\" value is not a number. Sum function can only be apply on number values.");
                    }
                } else {
                    System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                            "\" has not been found in event. Please check rule with id : " +
                            controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
                }
            }
        } else {
            System.out.println("Sum aggregation function need at least \"aggfield\" to be set.");
        }

        return collectEvent;
    }
}
