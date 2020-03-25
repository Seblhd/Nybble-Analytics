package com.nybble.alpha.event_stream;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class EventWindowFunction implements WindowFunction<ObjectNode, ObjectNode, ObjectNode, TimeWindow> {
    @Override
    public void apply(ObjectNode ruleKey, TimeWindow timeWindow, Iterable<ObjectNode> iterable, Collector<ObjectNode> collector) {
        // Collect each node from window after Trigger.
        for (ObjectNode ruleNode : iterable) {
            collector.collect(ruleNode);
        }
    }
}
