package com.nybble.alpha.alert_engine;

import com.nybble.alpha.alert_engine.aggregation_functions.CountFunction;
import com.nybble.alpha.alert_engine.aggregation_functions.MaxFunction;
import com.nybble.alpha.alert_engine.aggregation_functions.UniqueCountFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

public class MatchAggregation implements FlatMapFunction<Tuple2<ObjectNode, ObjectNode>, Tuple2<ObjectNode, ObjectNode>> {

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

            if (controlEventMatch.f1.get("rule").get(0).get("aggregation").get("aggfunction").asText().equals("count")) {
                boolean countAgg = new CountFunction().countEvent(controlEventMatch);

                if (countAgg) {
                    collector.collect(Tuple2.of(controlEventMatch.f0, controlEventMatch.f1));
                }
            } else if (controlEventMatch.f1.get("rule").get(0).get("aggregation").get("aggfunction").asText().equals("uniquecount")) {
                boolean uniqueCountAgg = new UniqueCountFunction().countEvent(controlEventMatch);

                if (uniqueCountAgg) {
                    collector.collect(Tuple2.of(controlEventMatch.f0, controlEventMatch.f1));
                }
            } else if (controlEventMatch.f1.get("rule").get(0).get("aggregation").get("aggfunction").asText().equals("max")) {
                boolean maxAgg = new MaxFunction().highestValue(controlEventMatch);

                if (maxAgg) {
                    collector.collect(Tuple2.of(controlEventMatch.f0, controlEventMatch.f1));
                }
            } else if (controlEventMatch.f1.get("rule").get(0).get("aggregation").get("aggfunction").asText().equals("min")) {
                boolean minAgg = new MaxFunction().highestValue(controlEventMatch);

                if (minAgg) {
                    collector.collect(Tuple2.of(controlEventMatch.f0, controlEventMatch.f1));
                }
            }
        }
    }
}
