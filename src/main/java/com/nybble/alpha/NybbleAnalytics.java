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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

		// Set up ElasticSearch environment
		List<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

		// Create a ElasticSearch sink where index is "events" to store events from Kafka.
		ElasticsearchSink.Builder<String> esSinkDataBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<String>() {
			public IndexRequest createIndexRequest(String element) throws IOException {
				ObjectMapper mapper = new ObjectMapper();
				HashMap eventNode = mapper.readValue(element, HashMap.class);

				return Requests.indexRequest()
						.index("events")
						.type("_doc")
						.source(eventNode);
			}

			@Override
			public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
				try {
					indexer.add(createIndexRequest(element));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		// Configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
		esSinkDataBuilder.setBulkFlushMaxActions(1);

		// Create Control Event (Sigma converted rules) Stream
		DataStream<ObjectNode> sigmaRuleSourceStream = env.addSource(new SigmaSourceFunction())
				.process(new MultipleRulesProcess())
				.keyBy(new ControlDynamicKey());
		sigmaRuleSourceStream.print();

		// Create Security Event Stream (From Kafka Stream)
		DataStream<ObjectNode> securityEventsStream = env.addSource(windowsLogsConsumer)
				.map(new MapFunction<ObjectNode, ObjectNode>() {
					@Override
					public ObjectNode map(ObjectNode eventNodes) throws Exception {
						return eventNodes.get("value").deepCopy();
					}
				});

		// Send Events to Elasticsearch
		securityEventsStream.map(ObjectNode::toString).addSink(esSinkDataBuilder.build());

		// Create Security Event (From Flink Stream) Stream and process for rule match.
		DataStream<ObjectNode> ruleEngineStream = securityEventsStream
				.process(new MultipleEventProcess())
				.keyBy(new EventDynamicKey())
				.timeWindow(Time.days(1))
				.trigger(new EventStreamTrigger())
				.apply(new EventWindowFunction());
		//windowsLogsStream.print();


		// Create a Sigma Alert Stream containing events filtered from rules.
		DataStream<ObjectNode> alertStream = sigmaRuleSourceStream
				.connect(ruleEngineStream)
				.flatMap(new LogSourceMatcher())
				.flatMap(new ControlEventMatcher())
				.flatMap(new MatchAggregation())
				.flatMap(new AlertCreation());
		alertStream.print();

		// execute program
		env.execute("Flink Nybble Analytics SIEM");
	}
}
