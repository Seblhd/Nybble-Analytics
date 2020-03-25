package com.nybble.alpha.event_stream;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class EventStreamTrigger extends Trigger<ObjectNode, Window> {

    private static Integer ruleBroadcastCount = -1;
    private static Integer ruleFileCount = 0;

    @Override
    public TriggerResult onElement(ObjectNode jsonNodes, long l, Window window, TriggerContext triggerContext) throws Exception {
        // To ensure that all events will be processed
        // All Sigma rules must has been broadcast to start the Event Stream.

        if (ruleBroadcastCount <= ruleFileCount) {
            // Continue Stream pause and put events in queue until at least one rule is broadcast.
            return TriggerResult.CONTINUE;
        } else {
            // Send events and remove them from the Windows.
            return TriggerResult.FIRE_AND_PURGE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long l, Window window, TriggerContext triggerContext) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onEventTime(long l, Window window, TriggerContext triggerContext) throws Exception {
        return null;
    }

    @Override
    public void clear(Window window, TriggerContext triggerContext) throws Exception {

    }

    public static void setRuleBroadcastCount(Integer broadcastedRules) {
        ruleBroadcastCount = broadcastedRules;
    }

    public static void setRuleFileCount(Integer countedRules) {
        ruleFileCount = countedRules;
    }
}
