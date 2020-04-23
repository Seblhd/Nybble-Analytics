package com.nybble.alpha.alert_engine.aggregation_functions;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.nybble.alpha.alert_engine.aggregation_functions.commons.AssertAggregationCondition;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class MinFunction {

    private static HashMap<ObjectNode, Tuple2<Date, Long>> fieldMinMap = new HashMap<>();
    private static HashMap<ObjectNode, Tuple2<Date, Long>> fieldByGroupMinMap = new HashMap<>();
    private ObjectMapper jsonMapper = new ObjectMapper();
    private static TimeZone tz = TimeZone.getTimeZone("UTC");
    private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private Configuration jsonPathConfig = Configuration.defaultConfiguration()
            .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
            .addOptions(Option.SUPPRESS_EXCEPTIONS);
    private static Logger alertEngineLogger = Logger.getLogger("alertEngineFile");

    public MinFunction() {
        // Set timezone for alert creation timestamp
        df.setTimeZone(tz);
    }

    public boolean lowestValue(Tuple2<ObjectNode, ObjectNode> controlEventMatch) throws JsonProcessingException, ParseException {

        boolean collectEvent = false;

        // Get aggregation node from rule
        ObjectNode aggregationNode = controlEventMatch.f1.get("rule").get(0).get("aggregation").deepCopy();

        if (aggregationNode.has("aggfield") && aggregationNode.has("groupfield")) {

            // Create fieldByGroupMinNode
            ObjectNode fieldByGroupMinNode = jsonMapper.createObjectNode();

            // Get value of groupfield from EventNode
            String groupfield = JsonPath.using(jsonPathConfig)
                    .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                    .read("$." + aggregationNode.get("groupfield").asText());

            if (groupfield != null) {

                // Add values in fieldByGroupMinNode. This Node is the fieldByGroupMinMap HashMap key.
                fieldByGroupMinNode.put("ruleid", controlEventMatch.f1.get("ruleid").asText());
                fieldByGroupMinNode.put("groupfield", groupfield);

                // If key already exists in fieldByGroupMinMap
                if (fieldByGroupMinMap.containsKey(fieldByGroupMinNode)) {

                    // Get event.created Date of current event.
                    Date currentEventDate = df.parse(controlEventMatch.f0.get("event").get("created").asText());
                    // Get event.created date of 1st event added in Map.
                    Date firstEventDate = fieldByGroupMinMap.get(fieldByGroupMinNode).f0;

                    // Get the number of seconds between firstEvent in Map and Current event time.
                    long timeGapSec = TimeUnit.MILLISECONDS.toSeconds(currentEventDate.getTime() - firstEventDate.getTime());

                    // Check if events is still in aggregation Timeframe.
                    // If Time gap is inferior to Timefram, then increment count by one and then check operator and value number.
                    // Else, delete entry in Map for this aggregation
                    if (timeGapSec < controlEventMatch.f1.get("rule").get(0).get("timeframe").get("duration").asLong()) {

                        // Get aggfield value from EventNode
                        String aggfield = JsonPath.using(jsonPathConfig)
                                .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                                .read("$." + aggregationNode.get("aggfield").asText()).toString();

                        if (aggfield != null) {
                            try {
                                if (Long.parseLong(aggfield) < fieldByGroupMinMap.get(fieldByGroupMinNode).f1) {
                                    // If event value is lowest, replace old highest value by lowest value from matched event
                                    fieldByGroupMinMap.get(fieldByGroupMinNode).f1 = Long.parseLong(aggfield);
                                }
                            } catch (NumberFormatException nb) {
                                System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() + "\" value is not a number. Min function can only be apply on number values.");
                                alertEngineLogger.error("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() + "\" value is not a number. Sum function can only be apply on number values.");
                            }
                        } else {
                            System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                                    "\" has not been found in event. Please check rule with id : " +
                                    controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");

                            alertEngineLogger.warn("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                                    "\" has not been found in event. Please check rule with id : "  +
                                    controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
                        }

                        //Check if aggregation condition has been met.
                        boolean conditionFlag = new AssertAggregationCondition().conditionResult(aggregationNode.get("aggoperator").asText(),
                                fieldByGroupMinMap.get(fieldByGroupMinNode).f1,
                                aggregationNode.get("aggvalue").asLong());

                        // If still in Time gap and aggregation condition is met, collect event to create alert and remove entry in Map
                        if (conditionFlag) {
                            collectEvent = true;
                            fieldByGroupMinMap.remove(fieldByGroupMinNode);
                        }
                    } else {
                        fieldByGroupMinMap.remove(fieldByGroupMinNode);
                    }
                } else {
                    // Else, create Tuple2 with 1st event.created timestamp and Min to value of 1st event.
                    Tuple2<Date, Long> aggregationTuple = new Tuple2<>();
                    aggregationTuple.f0 = df.parse(controlEventMatch.f0.get("event").get("created").asText());

                    // Get aggfield value from EventNode
                    String aggfield = JsonPath.using(jsonPathConfig)
                            .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                            .read("$." + aggregationNode.get("aggfield").asText()).toString();

                    if (aggfield != null) {
                        try {
                            aggregationTuple.f1 = Long.parseLong(aggfield);
                            // Then create a new entry in HashMap with fieldByGroupMinNode as Key and aggregationTuple as value.
                            fieldByGroupMinMap.put(fieldByGroupMinNode, aggregationTuple);
                        } catch (NumberFormatException nb) {
                            System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() + "\" value is not a number. Min function can only be apply on number values.");
                            alertEngineLogger.error("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() + "\" value is not a number. Sum function can only be apply on number values.");
                        }
                    } else {
                        System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                                "\" has not been found in event. Please check rule with id : " +
                                controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");

                        alertEngineLogger.warn("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                                "\" has not been found in event. Please check rule with id : "  +
                                controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
                    }
                }
            } else {
                System.out.println("\"group-field\":\"" + aggregationNode.get("groupfield").asText() +
                        "\" has not been found in event. Please check rule with id : " +
                        controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");


                alertEngineLogger.warn("\"group-field\":\"" + aggregationNode.get("groupfield").asText() +
                        "\" has not been found in event. Please check rule with id : " +
                        controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
            }
        } else if (aggregationNode.has("aggfield") && !aggregationNode.has("groupfield")) {

            // Create fieldMinNode
            ObjectNode fieldMinNode = jsonMapper.createObjectNode();

            // Add values in fieldMinNode. This Node is the fieldMinMap HashMap key.
            fieldMinNode.put("ruleid", controlEventMatch.f1.get("ruleid").asText());

            // If key already exists in fieldMinMap
            if (fieldMinMap.containsKey(fieldMinNode)) {
                // Get event.created Date of current event.
                Date currentEventDate = df.parse(controlEventMatch.f0.get("event").get("created").asText());
                // Get event.created date of 1st event added in Map.
                Date firstEventDate = fieldMinMap.get(fieldMinNode).f0;

                // Get the number of seconds between firstEvent in Map and Current event time.
                long timeGapSec = TimeUnit.MILLISECONDS.toSeconds(currentEventDate.getTime() - firstEventDate.getTime());

                // Check if events is still in aggregation Timeframe.
                // If Time gap is inferior to Timefram, then increment count by one and then check operator and value number.
                // Else, delete entry in Map for this aggregation
                if (timeGapSec < controlEventMatch.f1.get("rule").get(0).get("timeframe").get("duration").asLong()) {

                    // Get aggfield value from EventNode
                    String aggfield = JsonPath.using(jsonPathConfig)
                            .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                            .read("$." + aggregationNode.get("aggfield").asText()).toString();

                    if (aggfield != null) {
                        try {
                            if (Long.parseLong(aggfield) > fieldMinMap.get(fieldMinNode).f1) {
                                // If event value is highest, replace lowest value by highest value from matched event
                                fieldMinMap.get(fieldMinNode).f1 = Long.parseLong(aggfield);
                            }
                        } catch (NumberFormatException nb) {
                            System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() + "\" value is not a number. Min function can only be apply on number values.");
                            alertEngineLogger.error("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() + "\" value is not a number. Sum function can only be apply on number values.");
                        }
                    } else {
                        System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                                "\" has not been found in event. Please check rule with id : " +
                                controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");

                        alertEngineLogger.warn("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                                "\" has not been found in event. Please check rule with id : "  +
                                controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
                    }

                    //Check if aggregation condition has been met.
                    boolean conditionFlag = new AssertAggregationCondition().conditionResult(aggregationNode.get("aggoperator").asText(),
                            fieldMinMap.get(fieldMinNode).f1,
                            aggregationNode.get("aggvalue").asLong());

                    // If still in Time gap and aggregation condition is met, collect event to create alert and remove entry in Map
                    if (conditionFlag) {
                        collectEvent = true;
                        fieldMinMap.remove(fieldMinNode);
                    }
                } else {
                    fieldMinMap.remove(fieldMinNode);
                }
            } else {
                // Else, create Tuple2 with 1st event.created timestamp and Min to value of 1st event.
                Tuple2<Date, Long> aggregationTuple = new Tuple2<>();
                aggregationTuple.f0 = df.parse(controlEventMatch.f0.get("event").get("created").asText());

                // Get aggfield value from EventNode
                String aggfield = JsonPath.using(jsonPathConfig)
                        .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                        .read("$." + aggregationNode.get("aggfield").asText()).toString();

                if (aggfield != null) {
                    try {
                        aggregationTuple.f1 = Long.parseLong(aggfield);
                        // Then create a new entry in HashMap with fieldMinNode as Key and aggregationTuple as value.
                        fieldMinMap.put(fieldMinNode, aggregationTuple);
                    } catch (NumberFormatException nb) {
                        System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() + "\" value is not a number. Min function can only be apply on number values.");
                        alertEngineLogger.error("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() + "\" value is not a number. Sum function can only be apply on number values.");
                    }
                } else {
                    System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                            "\" has not been found in event. Please check rule with id : " +
                            controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");

                    alertEngineLogger.warn("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                            "\" has not been found in event. Please check rule with id : "  +
                            controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
                }
            }
        } else {
            System.out.println("Min aggregation function need at least \"aggfield\" to be set.");
            alertEngineLogger.error("Min aggregation function need at least \"aggfield\" to be set.");
        }

        return collectEvent;
    }
}
