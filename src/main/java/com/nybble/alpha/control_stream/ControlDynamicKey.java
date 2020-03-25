package com.nybble.alpha.control_stream;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class ControlDynamicKey implements KeySelector<ObjectNode, ObjectNode> {

    private static ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    public ObjectNode getKey(ObjectNode controlNode) throws Exception {

        // Remove all fields from logSourceKey before Key By definition
        ObjectNode logSourceKey = jsonMapper.createObjectNode();

        if (controlNode.get("rule").get(0).get("logsource").has("category")
                && controlNode.get("rule").get(0).get("logsource").has("product")) {
            // If event has both "Category" and "Product" fields
            // Then Key By "Category" and ""Product".
            logSourceKey.set("category", controlNode.get("rule").get(0).get("logsource").get("category"));
            logSourceKey.set("product", controlNode.get("rule").get(0).get("logsource").get("product"));
            return logSourceKey;
        } else if (controlNode.get("rule").get(0).get("logsource").has("product")) {
            // If event only has "Product" field
            // Then Key By "Product".
            logSourceKey.set("product", controlNode.get("rule").get(0).get("logsource").get("product"));
            return logSourceKey;
        } else if (controlNode.get("rule").get(0).get("logsource").has("category")) {
            // If event only has "Category" field
            // Then Key By "Category".
            logSourceKey.set("category", controlNode.get("rule").get(0).get("logsource").get("category"));
            return logSourceKey;
        } else {
            // If event has none of "Category" or "Product" fields
            // Then return an empty ObjectNode.
            return logSourceKey;
        }
    }
}