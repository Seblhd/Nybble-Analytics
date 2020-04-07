package com.nybble.alpha.alert_engine;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.HashMap;

public class LogSourceMatcher extends RichCoFlatMapFunction<ObjectNode, ObjectNode, Tuple2<ObjectNode, ArrayList<ObjectNode>>> {

    private static HashMap<ObjectNode, ArrayList<ObjectNode>> controlLogSourceMap = new HashMap<>();

    @Override
    public void flatMap1(ObjectNode controlNode, Collector<Tuple2<ObjectNode, ArrayList<ObjectNode>>> collector) throws Exception {

        ObjectNode logSourceNode = controlNode.get("rule").get(0).get("logsource").deepCopy();

        // Check if HashMap already contain key from ControlNode.
        if(controlLogSourceMap.containsKey(logSourceNode)) {
            // If already contains key, retrieve ArrayList with the corresponding key.
            ArrayList<ObjectNode> mapControlNodesList = controlLogSourceMap.get(logSourceNode);
            // If ControlNode doesn't exist in ArrayList, then add to ArrayList.
            if(!mapControlNodesList.contains(controlNode)) {
                mapControlNodesList.add(controlNode);
            // If ControlNode exists in ArrayList, then update value in ArrayList.
            } else {
                mapControlNodesList.set(mapControlNodesList.indexOf(controlNode), controlNode);
            }
            controlLogSourceMap.replace(logSourceNode, mapControlNodesList);
        } else {
            // If key doesn't existing in HashMap, then add key with ArrayList of corresponding ControlNode.
            ArrayList<ObjectNode> mapControlNodesList = new ArrayList<>();
            mapControlNodesList.add(controlNode);
            controlLogSourceMap.put(logSourceNode, mapControlNodesList);
        }
    }

    @Override
    public void flatMap2(ObjectNode eventNodes, Collector<Tuple2<ObjectNode, ArrayList<ObjectNode>>> collector) throws Exception {

        if (controlLogSourceMap.containsKey(eventNodes.get("logsource"))) {
            collector.collect(Tuple2.of(eventNodes, controlLogSourceMap.get(eventNodes.get("logsource"))));
        }
    }
}
