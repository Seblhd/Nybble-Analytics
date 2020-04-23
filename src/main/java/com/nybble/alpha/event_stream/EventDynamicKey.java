package com.nybble.alpha.event_stream;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;

public class EventDynamicKey implements KeySelector<ObjectNode, ObjectNode> {

    private static ObjectMapper jsonMapper = new ObjectMapper();
    private static Logger eventStreamLogger = Logger.getLogger("eventsStreamFile");

    @Override
    public ObjectNode getKey(ObjectNode eventNode) {

        // Remove all fields from logSourceKey before Key By definition
        ObjectNode logSourceKey = jsonMapper.createObjectNode();

        if (eventNode.get("logsource").has("category")
                && eventNode.get("logsource").has("product")) {
            // If event has both "Category" and "Product" fields
            // Then Key By "Category" and ""Product".
            logSourceKey.set("category", eventNode.get("logsource").get("category"));
            logSourceKey.set("product", eventNode.get("logsource").get("product"));
            return logSourceKey;
        } else if (eventNode.get("logsource").has("product")) {
            // If event only has "Product" field
            // Then Key By "Product".
            logSourceKey.set("product", eventNode.get("logsource").get("product"));
            return logSourceKey;
        } else if (eventNode.get("logsource").has("category")) {
            // If event only has "Category" field
            // Then Key By "Category".
            logSourceKey.set("category", eventNode.get("logsource").get("category"));
            return logSourceKey;
        } else {
            // If event has none of "Category" or "Product" fields
            // Then return an empty ObjectNode.
            eventStreamLogger.warn("Log source fields are missing. At least one of logsource.category or logsource.product is mandatory to match events against rule.\n" +
                    " Please review the source configuration for the following events : " + eventNode.toString());
            return logSourceKey;
        }
    }
}
