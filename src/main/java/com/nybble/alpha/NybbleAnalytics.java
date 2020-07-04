package com.nybble.alpha;

import com.nybble.alpha.control_stream.ControlDynamicKey;
import com.nybble.alpha.control_stream.MultipleRulesProcess;
import com.nybble.alpha.control_stream.SigmaSourceFunction;
import com.nybble.alpha.alert_engine.*;
import com.nybble.alpha.event_enrichment.EventAsyncEnricher;
import com.nybble.alpha.event_stream.EventDynamicKey;
import com.nybble.alpha.event_stream.EventStreamTrigger;
import com.nybble.alpha.event_stream.EventWindowFunction;
import com.nybble.alpha.event_stream.MultipleEventProcess;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.kafka.clients.admin.AdminClient;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class NybbleAnalytics {

	private static DateFormat esIndexFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static FlinkKafkaConsumer<ObjectNode> securityLogsConsumer;
	private static Integer totalKafkaTopicPartitions = 0;
	private static ObjectMapper jsonMapper = new ObjectMapper();
	private static SSLContext sslEs;

	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Initialize Nybble Flink configuration with value from Nybble Analytics Configuration.
		NybbleFlinkConfiguration.setNybbleConfiguration();

		// Get Nybble Flink configuration after initialization.
		Configuration nybbleFlinkConfiguration = NybbleFlinkConfiguration.getNybbleConfiguration();

		// Disable infinite DNS Cache for Application and set ttl to 60 secs.
		java.security.Security.setProperty("networkaddress.cache.ttl" , "60");

		// Disable infinite negative DNS Cache for Application and set ttl to 60 secs.
		java.security.Security.setProperty("networkaddress.cache.negative.ttl" , "60");

		// Get Elasticsearch REST Client Timeout values
		final Integer ES_REST_CON_TIMEOUT = nybbleFlinkConfiguration.getInteger(NybbleFlinkConfiguration.ELASTICSEARCH_REST_CONNECTION_TIMEOUT);
		final Integer ES_REST_REQ_TIMEOUT = nybbleFlinkConfiguration.getInteger(NybbleFlinkConfiguration.ELASTICSEARCH_REST_REQUEST_TIMEOUT);
		final Integer ES_REST_SCK_TIMEOUT = nybbleFlinkConfiguration.getInteger(NybbleFlinkConfiguration.ELASTICSEARCH_REST_SOCKET_TIMEOUT);

		// Get Elasticsearch indexes name for Event and Alert indexes.
		final String ES_EVENT_INDEX_NAME = nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.ELASTICSEARCH_EVENT_INDEX_NAME);
		final String ES_ALERT_INDEX_NAME = nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.ELASTICSEARCH_ALERT_INDEX_NAME);

		// Get Elasticsearch stream parallelism for Event and Alert indexes.
		final Integer ES_EVENT_STREAM_PARALLELISM = nybbleFlinkConfiguration.getInteger(NybbleFlinkConfiguration.ELASTICSEARCH_EVENT_STREAM_PARALLELISM);
		final Integer ES_ALERT_STREAM_PARALLELISM = nybbleFlinkConfiguration.getInteger(NybbleFlinkConfiguration.ELASTICSEARCH_ALERT_STREAM_PARALLELISM);

		final Boolean ES_SSL_ENABLE = nybbleFlinkConfiguration.getBoolean(NybbleFlinkConfiguration.ELASTICSEARCH_SSL_ENABLE);

		// If HTTPS is set for Elasticsearch Sink connection, then create SSL Context
		if (ES_SSL_ENABLE) {
			Path esTrustStorePath = Paths.get(nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.ELASTICSEARCH_TRUSTSTORE));
			KeyStore esTrustStore = KeyStore.getInstance("JKS");
			try (InputStream esTrustStoreInput = Files.newInputStream(esTrustStorePath)) {
				// Load truststore from Input Stream and get Password from configuration
				esTrustStore.load(esTrustStoreInput, nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.ELASTICSEARCH_TRUSTSTORE_PASSWORD).toCharArray());
			}
			SSLContextBuilder sslContext = SSLContexts.custom().loadTrustMaterial(esTrustStore, null);
			sslEs = sslContext.build();
		}

		// Start by creation of an ObjectNode containing information in MispMap JSON file.
		String mispMapNode = jsonMapper.readValue(new File(nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.MISP_MAP_PATH)), ObjectNode.class).toString();
		// Set MISP_MAP_VLAUES value in Nybble Flink configuration
		NybbleFlinkConfiguration.setMispMapValues(mispMapNode);

		// Set Event Enricher after init.
		EventAsyncEnricher eventAsyncEnricher = new EventAsyncEnricher();

		// Set up Kafka environment
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.KAFKA_BOOTSTRAP_SERVERS));
		kafkaProperties.setProperty("group.id", nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.KAFKA_GROUP_ID));

		// If SSL is enable, setup secure Kafka environment
		if (nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.KAFKA_SECURITY_PROTOCOL).equals("SSL")) {

			kafkaProperties.setProperty("security.protocol", nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.KAFKA_SECURITY_PROTOCOL));
			kafkaProperties.setProperty("ssl.enabled.protocols", nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.KAFKA_SSL_ENABLED_PROTOCOL));
			kafkaProperties.setProperty("ssl.truststore.location", nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.KAFKA_TRUSTSTORE_LOCATION));
			kafkaProperties.setProperty("ssl.truststore.password", nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.KAFKA_TRUSTSTORE_PASSWORD));
			kafkaProperties.setProperty("ssl.keystore.location", nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.KAFKA_KEYSTORE_LOCATION));
			kafkaProperties.setProperty("ssl.keystore.password", nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.KAFKA_KEYSTORE_PASSWORD));
			kafkaProperties.setProperty("ssl.key.password", nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.KAFKA_KEY_PASSWORD));
		}

		// Create a Kafka Admin Client
		AdminClient kafkaAdminClient = AdminClient.create(kafkaProperties);

		// Create a Kafka consumer with list of topics or topics regex from configuration file and using JSON Deserialization schema and properties provided above.
		JSONKeyValueDeserializationSchema logsSchema = new JSONKeyValueDeserializationSchema(false);

		String kafkaTopicsNameList = nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.KAFKA_TOPIC_NAME_LIST);
		String kafkaTopicsNameRegex = nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.KAFKA_TOPIC_NAME_REGEX);

		// Get topics from list or compile regex to get topics from regex.
		if (!kafkaTopicsNameList.equals("") && kafkaTopicsNameRegex.equals("")) {

			// Push all comma separated values from String to List.
			List<String> kafkaTopicsList = Arrays.asList(kafkaTopicsNameList.split("\\s*,\\s*"));
			// Get topics from list.
			securityLogsConsumer = new FlinkKafkaConsumer(kafkaTopicsList, logsSchema, kafkaProperties);

		} else if (kafkaTopicsNameList.equals("") && !kafkaTopicsNameRegex.equals("")) {

			// Compile pattern and autodiscover topics from pattern.
			securityLogsConsumer = new FlinkKafkaConsumer(java.util.regex.Pattern.compile(kafkaTopicsNameRegex), logsSchema, kafkaProperties);

		} else if (!kafkaTopicsNameList.equals("")) {

			// Topic list and topics pattern has both been provided. By default, use topic list.
			System.out.println("Topic list and topics pattern have both been provided. By default, topic list is used.");
			List<String> kafkaTopicsList = Arrays.asList(kafkaTopicsNameList.split(","));
			// Get topics from list.
			securityLogsConsumer = new FlinkKafkaConsumer(kafkaTopicsList, logsSchema, kafkaProperties);

		}

		// Find number of Total partitions for all topics from configuration file.
		if (!kafkaTopicsNameList.equals("")) {

			// Push all comma separated values from String to List.
			List<String> kafkaTopicsList = Arrays.asList(kafkaTopicsNameList.split("\\s*,\\s*"));

			kafkaTopicsList.forEach(topic -> {
				try {
					int topicSize = kafkaAdminClient.describeTopics(kafkaTopicsList).values().get(topic).get().partitions().size();
					totalKafkaTopicPartitions += topicSize;
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			});
		} else if (!kafkaTopicsNameRegex.equals("")) {
			// Get number of partitions per topic and add to total number of partitions for all topics.
			// Get list of all topics to find corresponding pattern ones and count partitions.
			Set<String> kafkaTopicsRegex = kafkaAdminClient.listTopics().names().get();
			ArrayList<String> foundTopics = new ArrayList<>();

			kafkaTopicsRegex.forEach(topics -> {
				Matcher topicPattern = Pattern.compile(kafkaTopicsNameRegex).matcher(topics);

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
		}

		// Set Kafka start position with value from config file.
		switch (nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.KAFKA_START_POSITION)) {
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
				if (nybbleFlinkConfiguration.getLong(NybbleFlinkConfiguration.KAFKA_START_EPOCH_TIMESTAMP) != 0L) {
					securityLogsConsumer.setStartFromTimestamp(nybbleFlinkConfiguration.getLong(NybbleFlinkConfiguration.KAFKA_START_EPOCH_TIMESTAMP));
				} else {
					System.out.println("Epoch timestamp in millisecond must be set to use start position from timestamp.");
				}
				break;
		}

		// Set up ElasticSearch environment
		List<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(new HttpHost(nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.ELASTICSEARCH_HOST),
				nybbleFlinkConfiguration.getInteger(NybbleFlinkConfiguration.ELASTICSEARCH_PORT),
				nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.ELASTICSEARCH_PROTO)));

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

			// If Elasticsearch authentication is enable, then configure Http Client to use credentials from Nybble configuration.
			if (nybbleFlinkConfiguration.getBoolean(NybbleFlinkConfiguration.ELASTICSEARCH_AUTH_ENABLE)) {
				restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {

						CredentialsProvider esCredentialsProvider = new BasicCredentialsProvider();
						esCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(
								nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.ELASTICSEARCH_USERNAME),
								nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.ELASTICSEARCH_PASSWORD)
						));

						if (ES_SSL_ENABLE) {
							return httpAsyncClientBuilder.setDefaultCredentialsProvider(esCredentialsProvider).setSSLContext(sslEs);
						} else {
							return httpAsyncClientBuilder.setDefaultCredentialsProvider(esCredentialsProvider);
						}
					}
				});
			}
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

			// If Elasticsearch authentication is enable, then configure Http Client to use credentials from Nybble configuration.
			if (nybbleFlinkConfiguration.getBoolean(NybbleFlinkConfiguration.ELASTICSEARCH_AUTH_ENABLE)) {
				restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {

						CredentialsProvider esCredentialsProvider = new BasicCredentialsProvider();
						esCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(
								nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.ELASTICSEARCH_USERNAME),
								nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.ELASTICSEARCH_PASSWORD)
						));

						if (ES_SSL_ENABLE) {
							return httpAsyncClientBuilder.setDefaultCredentialsProvider(esCredentialsProvider).setSSLContext(sslEs);
						} else {
							return httpAsyncClientBuilder.setDefaultCredentialsProvider(esCredentialsProvider);
						}
					}
				});
			}
		});

		// Configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
		esSinkDataBuilder.setBulkFlushMaxActions(nybbleFlinkConfiguration.getInteger(NybbleFlinkConfiguration.ELASTICSEARCH_EVENT_MAX_BULK_FLUSH_ACTION));
		esSinkAlertBuilder.setBulkFlushMaxActions(nybbleFlinkConfiguration.getInteger(NybbleFlinkConfiguration.ELASTICSEARCH_ALERT_MAX_BULK_FLUSH_ACTION));

		SigmaSourceFunction sigmaRulesSource = new SigmaSourceFunction();
		sigmaRulesSource.open();

		// Create Control Event (Sigma converted rules) Stream
		DataStream<ObjectNode> sigmaRuleSourceStream = env.addSource(sigmaRulesSource)
				.name("Control Stream : Sigma rules")
				.process(new MultipleRulesProcess())
				.keyBy(new ControlDynamicKey());
		sigmaRuleSourceStream.print();

		// Create Security Event Stream (From Kafka Stream)
		DataStream<ObjectNode> securityEventsStream = env.addSource(securityLogsConsumer)
				.name("Event Stream : Kafka consumer")
				.map(new MapFunction<ObjectNode, ObjectNode>() {
					@Override
					public ObjectNode map(ObjectNode eventNodes) {
						return eventNodes.get("value").deepCopy();
					}
				});

		DataStream<ObjectNode> securityEnrichEventsStream = AsyncDataStream.unorderedWait(securityEventsStream, eventAsyncEnricher, 5000, TimeUnit.MILLISECONDS).setParallelism(6);

		// Send Enriched Events to Elasticsearch
		securityEnrichEventsStream.map(ObjectNode::toString)
				.addSink(esSinkDataBuilder.build())
				.setParallelism(ES_EVENT_STREAM_PARALLELISM)
				.name("Elasticsearch Events");

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
		alertStream.map(Objects::toString)
				.addSink(esSinkAlertBuilder.build())
				.setParallelism(ES_ALERT_STREAM_PARALLELISM)
				.name("Elasticsearch Alerts");

		// execute program
		env.execute("Flink Nybble Analytics SIEM");
	}
}
