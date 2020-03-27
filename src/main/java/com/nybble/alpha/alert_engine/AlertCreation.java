package com.nybble.alpha.alert_engine;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.util.Collector;
import org.elasticsearch.common.UUIDs;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;


public class AlertCreation implements FlatMapFunction<Tuple2<ObjectNode, ObjectNode>, ObjectNode> {

    private ObjectMapper jsonMapper = new ObjectMapper();
    private static TimeZone tz = TimeZone.getTimeZone("UTC");
    private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    public AlertCreation() {
        // Set timezone for alert creation timestamp
        df.setTimeZone(tz);
    }

    @Override
    public void flatMap(Tuple2<ObjectNode, ObjectNode> controlEventMatch, Collector<ObjectNode> collector) throws Exception {

        // controlEventMatch.f0 is Event Node
        // controlEventMatch.f1 is ControlNode

        // Create an Alert Node to store Event Node and fields from ControlNode.
        ObjectNode alertNode = jsonMapper.createObjectNode();

        // Add processing time in alert
        alertNode.put("alert.processing.time", df.format(new Date()));

        // Add alert UUID with default Elasticsearch ID generator. Will be use for indexing too.
        alertNode.put("alert.uid", UUIDs.base64UUID());

        // Add Sigma rule ID
        alertNode.put("rule.id", controlEventMatch.f1.get("ruleid").asText());

        // Add Sigma rule title
        alertNode.put("rule.name", controlEventMatch.f1.get("ruletitle").asText());

        //Add Sigma rule tags array
        if (controlEventMatch.f1.has("tags")) {
            alertNode.putArray("tags").addAll((ArrayNode) controlEventMatch.f1.get("tags"));
        }

        // Add each useful fields if existing
        if (controlEventMatch.f1.has("fields")) {
            ArrayNode fieldsArrayNode = ((ArrayNode) controlEventMatch.f1.get("fields"));
            fieldsArrayNode.forEach(fields -> {
                if (hasField(controlEventMatch.f0, fields.asText())) {
                    alertNode.put(fields.asText() , controlEventMatch.f0.findValue(fields.asText()).asText());
                }
            });
        }

        // Add event that triggered alert
        alertNode.put("event", controlEventMatch.f0.toString());

        // Collect final AlertNode
        collector.collect(alertNode);
    }

    private boolean hasField(ObjectNode eventNode, String searchField) {
        // Create a Boolean for existing or not.
        boolean fieldExists = eventNode.has(searchField);

        if(!fieldExists) {
            Iterator<String> fieldIterator = eventNode.fieldNames();
            while(fieldIterator.hasNext()) {
                String nextField = fieldIterator.next();
                try {
                    if (eventNode.get(nextField) instanceof ObjectNode) {
                        fieldExists = hasField(eventNode.get(nextField).deepCopy(), nextField);
                    }

                    if (fieldExists) {
                        break;
                    }
                } finally {
                    System.out.println("End of search. Field has not been found in event. Please review Sigma rule and corresponding events.");
                }
            }
        }

        return fieldExists;
    }
}
