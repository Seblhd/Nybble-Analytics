package com.nybble.alpha.alert_engine;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.HashMap;

public class MatchAggregation implements FlatMapFunction<Tuple2<ObjectNode, ObjectNode>, Tuple2<ObjectNode, ObjectNode>> {

    private HashMap<ObjectNode, Tuple2<Date, Integer>> globalCountMap;
    private HashMap<ObjectNode, Tuple2<Date, Integer>> fieldCountMap;
    private HashMap<ObjectNode, Tuple2<Date, Integer>> fieldByGroupCountMap;
    private ObjectMapper jsonMapper = new ObjectMapper();
    private Configuration jsonPathConf;

    /*public MatchAggregation(Configuration jsonPathAggregationConf) {
        // Create JsonPath configuration to search value of fields.
        Configuration jsonPathConfig = Configuration.defaultConfiguration()
                .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
                .addOptions(Option.ALWAYS_RETURN_LIST)
                .addOptions(Option.SUPPRESS_EXCEPTIONS);
    }*/

    @Override
    public void flatMap(Tuple2<ObjectNode, ObjectNode> controlEventMatch, Collector<Tuple2<ObjectNode, ObjectNode>> collector) throws Exception {

        // controlEventMatch.f0 is Event Node
        // controlEventMatch.f1 is ControlNode

        // If aggregation and timeframe are empty, then collect events.
        if (controlEventMatch.f1.get("rule").get(0).get("aggregation").isNull()
                && controlEventMatch.f1.get("rule").get(0).get("timeframe").isNull()) {
            collector.collect(Tuple2.of(controlEventMatch.f0, controlEventMatch.f1));
        } else if (!controlEventMatch.f1.get("rule").get(0).get("aggregation").isNull()
                && !controlEventMatch.f1.get("rule").get(0).get("timeframe").isNull()) {

            // Get aggregation node from rule
            ObjectNode aggregationNode = controlEventMatch.f1.get("rule").get(0).get("aggregation").deepCopy();

            System.out.println("Aggregation node is : " + aggregationNode.toString());

            if (aggregationNode.has("aggfield") && aggregationNode.has("groupfield")) {

            } else if (aggregationNode.has("aggfield") && !aggregationNode.has("groupfield")) {

            } else if (!aggregationNode.has("aggfield") && !aggregationNode.has("groupfield")) {

            }
        }
    }
}
