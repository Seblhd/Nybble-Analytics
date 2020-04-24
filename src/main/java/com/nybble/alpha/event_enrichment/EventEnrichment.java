package com.nybble.alpha.event_enrichment;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class EventEnrichment implements MapFunction<ObjectNode, ObjectNode> {

    @Override
    public ObjectNode map(ObjectNode eventNode) throws Exception {
        return null;
    }
}
