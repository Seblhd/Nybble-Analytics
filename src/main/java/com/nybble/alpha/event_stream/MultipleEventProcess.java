package com.nybble.alpha.event_stream;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MultipleEventProcess extends ProcessFunction<ObjectNode, ObjectNode> {

    private ObjectMapper jsonMapper = new ObjectMapper();
    private static List<ObjectNode> sigmaLogSourceList = new ArrayList<>();

    @Override
    public void processElement(ObjectNode event, Context context, Collector<ObjectNode> collector) throws Exception {

        // In any case, collect event.
        // Events are collected if they have only one fields in "Logsource" and if will be ignored.
        // Events are collected with multiple fields if they have multiple fiels in "Logsource" and will be also
        // collected with each separate fields in the if block.
        collector.collect(event);

        if (event.get("logsource").size() > 1) {


            Iterator<Map.Entry<String, JsonNode>> logSourceFields = event.get("logsource").fields();

            while (logSourceFields.hasNext()) {
                // Retrieve fields from Iterator
                Map.Entry<String, JsonNode> fields = logSourceFields.next();
                // For each field in "Logsource", create a new node to replace multi fields by single field.
                ObjectNode singleFieldEvent = jsonMapper.createObjectNode();
                singleFieldEvent.set(fields.getKey(), fields.getValue());

                // Collect updated event with single field only if logsource field exist in at least one rule.
                if (sigmaLogSourceList.contains(singleFieldEvent)) {
                    event.set("logsource", singleFieldEvent);
                    collector.collect(event);
                }
            }
        }
    }

    public static void setSigmaLogSource(List<ObjectNode> LogSourceList) {
        sigmaLogSourceList = LogSourceList;
    }
}
