package com.nybble.alpha.alert_engine;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;
import com.jayway.jsonpath.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ControlEventMatcher implements FlatMapFunction<Tuple2<ObjectNode, ArrayList<ObjectNode>>, Tuple2<ObjectNode, ObjectNode>> {

    private ObjectMapper jsonMapper = new ObjectMapper();
    private String controlRule;

    @Override
    public void flatMap(Tuple2<ObjectNode, ArrayList<ObjectNode>> controlEventTuple, Collector<Tuple2<ObjectNode, ObjectNode>> collector) {

        Configuration jsonPathConfig = Configuration.defaultConfiguration().addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL).addOptions(Option.ALWAYS_RETURN_LIST).addOptions(Option.SUPPRESS_EXCEPTIONS);

        for (ObjectNode controlNode : controlEventTuple.f1) {
            try {

                controlRule = controlNode.get("rule").get(0).get("jsonpathrule").asText();

                // Works with this config.
                List<Map<String, ObjectNode>> eventMatch = JsonPath.using(jsonPathConfig)
                        .parse(jsonMapper.writeValueAsString(controlEventTuple.f0))
                        .read(controlRule);

                // Rule debug line
                //List<Map<String, ObjectNode>> eventMatch = JsonPath.read(controlEventTuple.f0.toString(), "$[?(@.event.code == 4738 && !((@.winlog.event_data.AllowedToDelegateTo == null || @.winlog.event_data.AllowedToDelegateTo == '-')))]");

                if (!eventMatch.isEmpty()) {
                    collector.collect(Tuple2.of(controlEventTuple.f0, controlNode));
                }

            } catch (Exception e) {
                System.out.println("Error on rule : " + controlRule);
                e.printStackTrace();
            }

        }
    }
}
