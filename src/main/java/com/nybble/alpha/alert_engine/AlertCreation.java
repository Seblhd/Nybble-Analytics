package com.nybble.alpha.alert_engine;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;
import org.elasticsearch.common.UUIDs;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
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
    public void flatMap(Tuple2<ObjectNode, ObjectNode> controlEventMatch, Collector<ObjectNode> collector) {

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

        alertNode.put("rule.status", controlEventMatch.f1.get("rulestatus").asText());

        //Add Sigma rule tags array
        if (controlEventMatch.f1.get("rule").get(0).has("tags")) {
            alertNode.putArray("tags").addAll((ArrayNode) controlEventMatch.f1.get("rule").get(0).get("tags"));
        }

        // Add each useful fields if existing
        if (controlEventMatch.f1.get("rule").get(0).has("fields")) {
            // Put all fields from Control Event in an ArrayNode
            ArrayNode fieldsArrayNode = ((ArrayNode) controlEventMatch.f1.get("rule").get(0).get("fields"));
            // Create JsonPath configuration to search value of fields.
            Configuration jsonPathConfig = Configuration.defaultConfiguration()
                    .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
                    .addOptions(Option.ALWAYS_RETURN_LIST)
                    .addOptions(Option.SUPPRESS_EXCEPTIONS);

            fieldsArrayNode.forEach(fields -> {
                try {
                    // Search if field is in Event Node
                    List<?> searchFieldValue = JsonPath.using(jsonPathConfig)
                            .parse(jsonMapper.writeValueAsString(controlEventMatch.f0))
                            .read("$." + fields.asText());

                    // If field has been found, then searchField value list contain the value.
                    // Else, because of the "ALWAYS_RETURN_LIST" option, empty list is return when field has not bee found.
                    if (!searchFieldValue.isEmpty()) {
                        searchFieldValue.forEach(foundFields -> {
                            if (foundFields != null) {
                                alertNode.put(fields.asText(), foundFields.toString());
                            }
                        });
                    } else {
                        System.out.println("Field \""+ fields.toString() +"\" has not been found in event. Please review Sigma rule and corresponding events.");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // Add event that triggered alert
        alertNode.put("rule.event", controlEventMatch.f0.toString());

        // Collect final AlertNode
        collector.collect(alertNode);
    }
}
