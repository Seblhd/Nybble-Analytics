package com.nybble.alpha;

import com.nybble.alpha.control_stream.ControlDynamicKey;
import com.nybble.alpha.control_stream.MultipleRulesProcess;
import com.nybble.alpha.control_stream.SigmaSourceFunction;
import com.nybble.alpha.alert_engine.*;
import com.nybble.alpha.event_enrichment.EventEnrichment;
import com.nybble.alpha.event_enrichment.MipsEnrichment;
import com.nybble.alpha.event_stream.EventDynamicKey;
import com.nybble.alpha.event_stream.EventStreamTrigger;
import com.nybble.alpha.event_stream.EventWindowFunction;
import com.nybble.alpha.event_stream.MultipleEventProcess;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class NybbleAnalytics {

	private static DateFormat esIndexFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static FlinkKafkaConsumer<ObjectNode> securityLogsConsumer;
	private static Integer totalKafkaTopicPartitions = 0;

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Get configuration from config file.
		NybbleAnalyticsConfiguration nybbleAnalyticsConfiguration = new NybbleAnalyticsConfiguration();

		// Get Elasticsearch REST Client Timeout values
		final Integer ES_REST_CON_TIMEOUT = nybbleAnalyticsConfiguration.getElasticsearchRestConTimeOut();
		final Integer ES_REST_REQ_TIMEOUT = nybbleAnalyticsConfiguration.getElasticsearchRestReqTimeOut();
		final Integer ES_REST_SCK_TIMEOUT = nybbleAnalyticsConfiguration.getElasticsearchRestSckTimeOut();

		// Get Elasticsearch indexes name for Event and Alert indexes.
		final String ES_EVENT_INDEX_NAME = nybbleAnalyticsConfiguration.getElasticsearchEventIndex();
		final String ES_ALERT_INDEX_NAME = nybbleAnalyticsConfiguration.getElasticsearchAlertIndex();

		// Set mapping for MISP enrichement.
		new MipsEnrichment().setMispMapping();

		// Set up Kafka environment
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", nybbleAnalyticsConfiguration.getKafkaBootstrapServers());
		kafkaProperties.setProperty("group.id", nybbleAnalyticsConfiguration.getKafkaGroupId());

		// Create a Kafka Admin Client
		AdminClient kafkaAdminClient = AdminClient.create(kafkaProperties);

		// Create a Kafka consumer where topic is "windows-logs", using JSON Deserialization schema and properties provided above. Read from the beginning.
		JSONKeyValueDeserializationSchema logsSchema = new JSONKeyValueDeserializationSchema(false);

		//Check if topic list is pattern-based or is a list of topics.
		if (nybbleAnalyticsConfiguration.getKafkaTopicsName() != null &&
				nybbleAnalyticsConfiguration.getKafkaTopicsPattern() == null) {
			// Get topics from list.
			securityLogsConsumer = new FlinkKafkaConsumer(nybbleAnalyticsConfiguration.getKafkaTopicsName(), logsSchema, kafkaProperties);
			// Get number of partitions per topic and add to total number of partitions for all topics.
			nybbleAnalyticsConfiguration.getKafkaTopicsName().forEach(topic -> {
				try {
					int topicSize = kafkaAdminClient.describeTopics(nybbleAnalyticsConfiguration.getKafkaTopicsName()).values().get(topic).get().partitions().size();
					totalKafkaTopicPartitions += topicSize;
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			});
		} else if (nybbleAnalyticsConfiguration.getKafkaTopicsName() == null &&
				nybbleAnalyticsConfiguration.getKafkaTopicsPattern() != null) {
			// Compile pattern and autodiscover topics from pattern.
			securityLogsConsumer = new FlinkKafkaConsumer(java.util.regex.Pattern.compile(nybbleAnalyticsConfiguration.getKafkaTopicsPattern()), logsSchema, kafkaProperties);
			// Get number of partitions per topic and add to total number of partitions for all topics.

			// Get list of all topics to find corresponding pattern ones and count partitions.
			Set<String> kafkaTopics = kafkaAdminClient.listTopics().names().get();
			ArrayList<String> foundTopics = new ArrayList<>();

			kafkaTopics.forEach(topics -> {
				Matcher topicPattern = Pattern.compile(nybbleAnalyticsConfiguration.getKafkaTopicsPattern()).matcher(topics);

				while (topicPattern.find()) {
					foundTopics.add(topicPattern.group(0));
				}
			});

			foundTopics.forEach(topic -> {
				try {
					int topicSize = kafkaAdminClient.describeTopics(foundTopics).values().get(topic).get().partitions().size();
					totalKafkaTopicPartitions += topicSize;
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			});

			//if (totalKafkaTopicPartitions <= env.getParallelism()) {
			//	securityLogsConsumer.getRuntimeContext().getExecutionConfig().setParallelism(totalKafkaTopicPartitions);
			//}
		} else if (nybbleAnalyticsConfiguration.getKafkaTopicsName() != null &&
				nybbleAnalyticsConfiguration.getKafkaTopicsPattern() != null) {
			// Topic list and topics pattern has both been provided. By default, use topic list.
			System.out.println("Topic list and topics pattern have both been provided. By default, topic list is used.");
			securityLogsConsumer = new FlinkKafkaConsumer(nybbleAnalyticsConfiguration.getKafkaTopicsName(), logsSchema, kafkaProperties);
			// Get number of partitions per topic and add to total number of partitions for all topics.
			nybbleAnalyticsConfiguration.getKafkaTopicsName().forEach(topic -> {
				try {
					int topicSize = kafkaAdminClient.describeTopics(nybbleAnalyticsConfiguration.getKafkaTopicsName()).values().get(topic).get().partitions().size();
					totalKafkaTopicPartitions += topicSize;
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			});
		}

		switch (nybbleAnalyticsConfiguration.getStartPosition()) {
			case "setStartFromEarliest":
				securityLogsConsumer.setStartFromEarliest();
				break;
			case "setStartFromLatest":
				securityLogsConsumer.setStartFromLatest();
				break;
			case "setStartFromGroupOffsets":
				securityLogsConsumer.setStartFromGroupOffsets();
				break;
			case "setStartFromTimestamp":
				if (nybbleAnalyticsConfiguration.getStartEpochTimestamp() != null) {
					securityLogsConsumer.setStartFromTimestamp(nybbleAnalyticsConfiguration.getStartEpochTimestamp());
				} else {
					System.out.println("Epoch timestamp in millisecond must be set to use start position from timestamp.");
				}
				break;
		}

		// Set up ElasticSearch environment
		List<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(new HttpHost(nybbleAnalyticsConfiguration.getElasticsearchHost(),
				nybbleAnalyticsConfiguration.getElasticsearchPort(),
				nybbleAnalyticsConfiguration.getElasticsearchProto()));

		// Create a ElasticSearch sink where index is "events" to store events from Kafka.
		ElasticsearchSink.Builder<String> esSinkDataBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<String>() {
			public IndexRequest createIndexRequest(String element) throws IOException {
				ObjectMapper mapper = new ObjectMapper();
				HashMap eventNode = mapper.readValue(element, HashMap.class);

				return Requests.indexRequest()
						.index(ES_EVENT_INDEX_NAME + esIndexFormat.format(new Date()))
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

		// Create a ElasticSearch sink where index is "events" to store events from Kafka.
		ElasticsearchSink.Builder<String> esSinkAlertBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<String>() {
			public IndexRequest createIndexRequest(String element) throws IOException {
				ObjectMapper mapper = new ObjectMapper();
				HashMap alertNode = mapper.readValue(element, HashMap.class);

				return Requests.indexRequest()
						.index(ES_ALERT_INDEX_NAME + alertNode.get("rule.status").toString() + "-" + esIndexFormat.format(new Date()))
						.id(alertNode.get("alert.uid").toString())
						.source(alertNode);
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

		// Configuration for the REST Client; Timeout values can be modified to avoid time out on Elasticsearch node with small amount of memory.
		esSinkAlertBuilder.setRestClientFactory(restClientBuilder -> {
			restClientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
				@Override
				public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder builder) {
					return builder.setConnectTimeout(ES_REST_CON_TIMEOUT)
							.setSocketTimeout(ES_REST_SCK_TIMEOUT)
							.setConnectionRequestTimeout(ES_REST_SCK_TIMEOUT);
				}
			});
		});

		esSinkDataBuilder.setRestClientFactory(restClientBuilder -> {
			restClientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
				@Override
				public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder builder) {
					return builder.setConnectTimeout(ES_REST_CON_TIMEOUT)
							.setSocketTimeout(ES_REST_SCK_TIMEOUT)
							.setConnectionRequestTimeout(ES_REST_REQ_TIMEOUT);
				}
			});
		});

		// Configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
		esSinkDataBuilder.setBulkFlushMaxActions(1);
		esSinkAlertBuilder.setBulkFlushMaxActions(1);

		// Create Control Event (Sigma converted rules) Stream
		DataStream<ObjectNode> sigmaRuleSourceStream = env.addSource(new SigmaSourceFunction())
				.process(new MultipleRulesProcess())
				.keyBy(new ControlDynamicKey());
		sigmaRuleSourceStream.print();

		// Create Security Event Stream (From Kafka Stream)
		DataStream<ObjectNode> securityEventsStream = env.addSource(securityLogsConsumer)
				.map(new MapFunction<ObjectNode, ObjectNode>() {
					@Override
					public ObjectNode map(ObjectNode eventNodes) throws Exception {
						return eventNodes.get("value").deepCopy();
					}
				}).map(new EventEnrichment());

		// Send Events to Elasticsearch
		securityEventsStream.map(ObjectNode::toString).addSink(esSinkDataBuilder.build());

		// Create Security Event (From Flink Stream) Stream and process for rule match.
		DataStream<ObjectNode> ruleEngineStream = securityEventsStream
				.process(new MultipleEventProcess())
				.keyBy(new EventDynamicKey())
				.timeWindow(Time.days(1))
				.trigger(new EventStreamTrigger())
				.apply(new EventWindowFunction());
		//ruleEngineStream.print();

		// Create a Sigma Alert Stream containing events filtered from rules.
		DataStream<ObjectNode> alertStream = sigmaRuleSourceStream
				.connect(ruleEngineStream)
				.flatMap(new LogSourceMatcher())
				.flatMap(new ControlEventMatcher())
				.flatMap(new MatchAggregation())
				.flatMap(new AlertCreation());
		alertStream.print();

		// Send alerts to Elasticsearch
		alertStream.map(Objects::toString).addSink(esSinkAlertBuilder.build());

		// execute program
		env.execute("Flink Nybble Analytics SIEM");
	}
}
