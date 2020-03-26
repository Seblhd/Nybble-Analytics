package com.nybble.alpha;

import com.nybble.alpha.control_stream.ControlDynamicKey;
import com.nybble.alpha.control_stream.MultipleRulesProcess;
import com.nybble.alpha.control_stream.SigmaSourceFunction;
import com.nybble.alpha.alert_engine.*;

import com.nybble.alpha.event_stream.EventDynamicKey;
import com.nybble.alpha.event_stream.EventStreamTrigger;
import com.nybble.alpha.event_stream.EventWindowFunction;
import com.nybble.alpha.event_stream.MultipleEventProcess;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import java.util.Properties;



public class NybbleAnalytics {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Set up Kafka environment
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
		kafkaProperties.setProperty("group.id", "flink_kafka_consumer");

		// Create a Kafka consumer where topic is "windows-logs", using JSON Deserialization schema and properties provided above. Read from the beginning.
		JSONKeyValueDeserializationSchema logsSchema = new JSONKeyValueDeserializationSchema(false);
		FlinkKafkaConsumer<ObjectNode> windowsLogsConsumer = new FlinkKafkaConsumer("windows-logs", logsSchema, kafkaProperties);
		windowsLogsConsumer.setStartFromEarliest();

		// Create Control Event (Sigma converted rules) Stream
		DataStream<ObjectNode> sigmaRuleSourceStream = env.addSource(new SigmaSourceFunction())
				.process(new MultipleRulesProcess())
				.keyBy(new ControlDynamicKey());
		sigmaRuleSourceStream.print();

		// Create Security Event (From Kafka) Stream
		DataStream<ObjectNode> windowsLogsStream = env.addSource(windowsLogsConsumer)
				.map(new MapFunction<ObjectNode, ObjectNode>() {
					@Override
					public ObjectNode map(ObjectNode eventNodes) throws Exception {
						return eventNodes.get("value").deepCopy();
					}
				})
				.process(new MultipleEventProcess())
				.keyBy(new EventDynamicKey())
				.timeWindow(Time.days(1))
				.trigger(new EventStreamTrigger())
				.apply(new EventWindowFunction());
		//windowsLogsStream.print();


		// Create a Sigma Alert Stream containing events filtered from rules.
		DataStream<ObjectNode> alertStream = sigmaRuleSourceStream
				.connect(windowsLogsStream)
				.flatMap(new LogSourceMatcher())
				.flatMap(new ControlEventMatcher());
		alertStream.print();

		// execute program
		env.execute("Flink Nybble Analytics SIEM");
	}
}
