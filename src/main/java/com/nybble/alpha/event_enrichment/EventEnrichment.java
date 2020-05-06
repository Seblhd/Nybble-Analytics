package com.nybble.alpha.event_enrichment;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.HashMap;

public class EventEnrichment implements MapFunction<ObjectNode, ObjectNode> {

    private ArrayList<Tuple3<String, String, String>> enrichableFields = new ArrayList<>();
    private static Boolean mispEnabledFlag = false;

    @Override
    public ObjectNode map(ObjectNode eventNode) throws Exception {

        enrichableFields = new FindEnrichableFields().getList(eventNode);

        return eventNode;
    }

    public static void setMispEnabledFlag(Boolean enabledFlag) {
        mispEnabledFlag = enabledFlag;
    }
}
