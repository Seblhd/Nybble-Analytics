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

public class CountFunction {

    private static HashMap<ObjectNode, Tuple2<Date, Long>> globalCountMap = new HashMap<>();
    private static HashMap<ObjectNode, Tuple2<Date, Long>> fieldCountMap = new HashMap<>();
    private static HashMap<ObjectNode, Tuple2<Date, Long>> fieldByGroupCountMap = new HashMap<>();
    private ObjectMapper jsonMapper = new ObjectMapper();
    private static TimeZone tz = TimeZone.getTimeZone("UTC");
    private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private Configuration jsonPathConfig = Configuration.defaultConfiguration()
            .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
            .addOptions(Option.SUPPRESS_EXCEPTIONS);
    private static Logger alertEngineLogger = Logger.getLogger("alertEngineFile");

    public CountFunction() {
        // Set timezone for alert creation timestamp
        df.setTimeZone(tz);
    }

    public boolean countEvent(Tuple2<ObjectNode, ObjectNode> controlEventMatch) throws JsonProcessingException, ParseException {

        boolean collectEvent = false;

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

                // If key already exists in fieldByGroupCountMap
                if (fieldByGroupCountMap.containsKey(fieldByGroupCountNode)) {

                    // Get event.created Date of current event.
                    Date currentEventDate = df.parse(controlEventMatch.f0.get("event").get("created").asText());
                    // Get event.created date of 1st event added in Map.
                    Date firstEventDate = fieldByGroupCountMap.get(fieldByGroupCountNode).f0;

                    // Get the number of seconds between firstEvent in Map and Current event time.
                    long timeGapSec = TimeUnit.MILLISECONDS.toSeconds(currentEventDate.getTime() - firstEventDate.getTime());

                    // Check if events is still in aggregation Timeframe.
                    // If Time gap is inferior to Timefram, then increment count by one and then check operator and value number.
                    // Else, delete entry in Map for this aggregation
                    if (timeGapSec < controlEventMatch.f1.get("rule").get(0).get("timeframe").get("duration").asLong()) {

                        // Increment the number of match event
                        fieldByGroupCountMap.get(fieldByGroupCountNode).f1 = fieldByGroupCountMap.get(fieldByGroupCountNode).f1+=1;

                        //Check if aggregation condition has been met.
                        boolean conditionFlag = new AssertAggregationCondition().conditionResult(aggregationNode.get("aggoperator").asText(),
                                fieldByGroupCountMap.get(fieldByGroupCountNode).f1,
                                aggregationNode.get("aggvalue").asLong());

                        // If still in Time gap and aggregation condition is met, collect event to create alert and remove entry in Map
                        if (conditionFlag) {
                            collectEvent = true;
                            fieldByGroupCountMap.remove(fieldByGroupCountNode);
                        }
                    } else {
                        fieldByGroupCountMap.remove(fieldByGroupCountNode);
                    }
                } else {
                    // Else, create Tuple2 with 1st event.created timestamp and count with value to 1.
                    Tuple2<Date, Long> aggregationTuple = new Tuple2<>();
                    aggregationTuple.f0 = df.parse(controlEventMatch.f0.get("event").get("created").asText());
                    aggregationTuple.f1 = 1L;

                    // Then create a new entry in HashMap with fieldByGroupCountNode as Key and aggregationTuple as value.
                    fieldByGroupCountMap.put(fieldByGroupCountNode, aggregationTuple);
                }
            } else {
                System.out.println("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                        "\" or \"group-field\":\"" + aggregationNode.get("groupfield").asText() +
                        "\" has not been found in event. Please check rule with id : " +
                        controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");

                alertEngineLogger.warn("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                        "\" or \"group-field\":\"" + aggregationNode.get("groupfield").asText() +
                        "\" has not been found in event. Please check rule with id : "  +
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

                if (fieldCountMap.containsKey(fieldCountNode)) {

                    // Get event.created Date of current event.
                    Date currentEventDate = df.parse(controlEventMatch.f0.get("event").get("created").asText());
                    // Get event.created date of 1st event added in Map.
                    Date firstEventDate = fieldCountMap.get(fieldCountNode).f0;

                    // Get the number of seconds between firstEvent in Map and Current event time.
                    long timeGapSec = TimeUnit.MILLISECONDS.toSeconds(currentEventDate.getTime() - firstEventDate.getTime());

                    // Check if events is still in aggregation Timeframe.
                    // If Time gap is inferior to Timefram, then increment count by one and then check operator and value number.
                    // Else, delete entry in Map for this aggregation
                    if (timeGapSec < controlEventMatch.f1.get("rule").get(0).get("timeframe").get("duration").asLong()) {

                        // Increment the number of match event
                        fieldCountMap.get(fieldCountNode).f1 = fieldCountMap.get(fieldCountNode).f1+=1;

                        //Check if aggregation condition has been met.
                        boolean conditionFlag = new AssertAggregationCondition().conditionResult(aggregationNode.get("aggoperator").asText(),
                                fieldCountMap.get(fieldCountNode).f1,
                                aggregationNode.get("aggvalue").asLong());

                        // If still in Time gap and aggregation condition is met, collect event to create alert and remove entry in Map
                        if (conditionFlag) {
                            collectEvent = true;
                            fieldCountMap.remove(fieldCountNode);
                        }
                    } else {
                        fieldCountMap.remove(fieldCountNode);
                    }
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

                alertEngineLogger.warn("\"aggfield\":\"" + aggregationNode.get("aggfield").asText() +
                        "\" has not been found in event. Please check rule with id : "  +
                        controlEventMatch.f1.get("ruleid").asText() + " and corresponding events.");
            }

        } else if (!aggregationNode.has("aggfield") && !aggregationNode.has("groupfield")) {

            // Create globalCountNode
            ObjectNode globalCountNode = jsonMapper.createObjectNode();

            // Add values in globalCountNode. This Node is the globalCountMap HashMap key.
            globalCountNode.put("ruleid", controlEventMatch.f1.get("ruleid").asText());

            if (globalCountMap.containsKey(globalCountNode)) {

                // Get event.created Date of current event.
                Date currentEventDate = df.parse(controlEventMatch.f0.get("event").get("created").asText());
                // Get event.created date of 1st event added in Map.
                Date firstEventDate = globalCountMap.get(globalCountNode).f0;

                // Get the number of seconds between firstEvent in Map and Current event time.
                long timeGapSec = TimeUnit.MILLISECONDS.toSeconds(currentEventDate.getTime() - firstEventDate.getTime());

                // Check if events is still in aggregation Timeframe.
                // If Time gap is inferior to Timefram, then increment count by one and then check operator and value number.
                // Else, delete entry in Map for this aggregation
                if (timeGapSec < controlEventMatch.f1.get("rule").get(0).get("timeframe").get("duration").asLong()) {

                    // Increment the number of match event
                    globalCountMap.get(globalCountNode).f1 = globalCountMap.get(globalCountNode).f1+=1;

                    //Check if aggregation condition has been met.
                    boolean conditionFlag = new AssertAggregationCondition().conditionResult(aggregationNode.get("aggoperator").asText(),
                            globalCountMap.get(globalCountNode).f1,
                            aggregationNode.get("aggvalue").asLong());

                    // If still in Time gap and aggregation condition is met, collect event to create alert and remove entry in Map
                    if (conditionFlag) {
                        collectEvent = true;
                        globalCountMap.remove(globalCountNode);
                    }
                } else {
                    globalCountMap.remove(globalCountNode);
                }
            } else {
                // Else, create Tuple2 with 1st event.created timestamp and count with value to 1.
                Tuple2<Date, Long> aggregationTuple = new Tuple2<>();
                aggregationTuple.f0 = df.parse(controlEventMatch.f0.get("event").get("created").asText());
                aggregationTuple.f1 = 1L;

                // Then create a new entry in HashMap with globalCountNode as Key and aggregationTuple as value.
                globalCountMap.put(globalCountNode, aggregationTuple);
            }
        }

        return collectEvent;
    }
}
