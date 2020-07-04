package com.nybble.alpha;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

public class NybbleFlinkConfiguration {

    //####################################################################################
    //#=========================== Redis specific options ===============================#
    //####################################################################################

    public static final ConfigOption<String> REDIS_SERVER_HOST =
            ConfigOptions.key("redis.server.host").stringType().defaultValue("localhost");

    public static final ConfigOption<Integer> REDIS_SERVER_PORT =
            ConfigOptions.key("redis.server.port").intType().defaultValue(6379);

    public static final ConfigOption<Integer> REDIS_SERVER_CONNECTION_TIMEOUT =
            ConfigOptions.key("redis.server.connection.timeout").intType().defaultValue(3000);

    public static final ConfigOption<Long> REDIS_MISP_CACHE_KEY_EXPIRATION =
            ConfigOptions.key("redis.misp.key.expire").longType().defaultValue(86400L);

    public static final ConfigOption<Long> REDIS_DNS_CACHE_KEY_EXPIRATION =
            ConfigOptions.key("redis.dns.key.expire").longType().defaultValue(86400L);

    public static final ConfigOption<Integer> REDIS_MISP_CACHE_DB_ID =
            ConfigOptions.key("redis.misp.cache.id").intType().defaultValue(0);

    public static final ConfigOption<Integer> REDIS_DNS_CACHE_DB_ID =
            ConfigOptions.key("redis.dns.cache.id").intType().defaultValue(1);

    public static final ConfigOption<Integer> REDIS_IO_THREADS_NUM =
            ConfigOptions.key("redis.io.threads").intType().noDefaultValue();

    public static final ConfigOption<Integer> REDIS_COMPUTE_THREADS_NUM =
            ConfigOptions.key("redis.compute.threads").intType().noDefaultValue();

    //####################################################################################
    //#=========================== Kafka specific options ===============================#
    //####################################################################################

    public static final ConfigOption<String> KAFKA_BOOTSTRAP_SERVERS =
            ConfigOptions.key("kafka.bootstrap.servers").stringType().defaultValue("localhost:9092");

    public static final ConfigOption<String> KAFKA_GROUP_ID =
            ConfigOptions.key("bootstrap.servers.port").stringType().defaultValue("flink_kafka_consumer");

    public static final ConfigOption<String> KAFKA_SECURITY_PROTOCOL =
            ConfigOptions.key("kafka.security.protocol").stringType().defaultValue("none");

    public static final ConfigOption<String> KAFKA_SSL_ENABLED_PROTOCOL =
            ConfigOptions.key("kafka.ssl.enabled.protocols").stringType().noDefaultValue();

    public static final ConfigOption<String> KAFKA_TRUSTSTORE_LOCATION =
            ConfigOptions.key("kafka.ssl.truststore.location").stringType().noDefaultValue();

    public static final ConfigOption<String> KAFKA_TRUSTSTORE_PASSWORD =
            ConfigOptions.key("kafka.ssl.truststore.password").stringType().noDefaultValue();

    public static final ConfigOption<String> KAFKA_KEYSTORE_LOCATION =
            ConfigOptions.key("kafka.ssl.keystore.location").stringType().noDefaultValue();

    public static final ConfigOption<String> KAFKA_KEYSTORE_PASSWORD =
            ConfigOptions.key("kafka.ssl.keystore.password").stringType().noDefaultValue();

    public static final ConfigOption<String> KAFKA_KEY_PASSWORD =
            ConfigOptions.key("kafka.ssl.key.password").stringType().noDefaultValue();

    public static final ConfigOption<String> KAFKA_TOPIC_NAME_LIST =
            ConfigOptions.key("kafka.topic.name.list").stringType().noDefaultValue();

    public static final ConfigOption<String> KAFKA_TOPIC_NAME_REGEX =
            ConfigOptions.key("kafka.topic.name.regex").stringType().noDefaultValue();

    public static final ConfigOption<String> KAFKA_START_POSITION =
            ConfigOptions.key("kafka.start.position").stringType().defaultValue("setStartFromGroupOffsets");

    public static final ConfigOption<Long> KAFKA_START_EPOCH_TIMESTAMP =
            ConfigOptions.key("kafka.start.timestamp").longType().noDefaultValue();

    //####################################################################################
    //#======================= Elasticsearch specific options ===========================#
    //####################################################################################

    public static final ConfigOption<String> ELASTICSEARCH_HOST =
            ConfigOptions.key("elasticsearch.host").stringType().defaultValue("localhost");

    public static final ConfigOption<Integer> ELASTICSEARCH_PORT =
            ConfigOptions.key("elasticsearch.port").intType().defaultValue(9200);

    public static final ConfigOption<String> ELASTICSEARCH_PROTO =
            ConfigOptions.key("elasticsearch.proto").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> ELASTICSEARCH_SSL_ENABLE =
            ConfigOptions.key("elasticsearch.ssl.enable").booleanType().defaultValue(false);

    public static final ConfigOption<String> ELASTICSEARCH_TRUSTSTORE =
            ConfigOptions.key("elasticsearch.truststore").stringType().noDefaultValue();

    public static final ConfigOption<String> ELASTICSEARCH_TRUSTSTORE_PASSWORD =
            ConfigOptions.key("elasticsearch.truststore.password").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> ELASTICSEARCH_AUTH_ENABLE =
            ConfigOptions.key("elasticsearch.auth.enable").booleanType().defaultValue(false);

    public static final ConfigOption<String> ELASTICSEARCH_USERNAME =
            ConfigOptions.key("elasticsearch.username").stringType().noDefaultValue();

    public static final ConfigOption<String> ELASTICSEARCH_PASSWORD =
            ConfigOptions.key("elasticsearch.password").stringType().noDefaultValue();

    public static final ConfigOption<String> ELASTICSEARCH_ALERT_INDEX_NAME =
            ConfigOptions.key("elasticsearch.alert.index").stringType().defaultValue("alerts-");

    public static final ConfigOption<String> ELASTICSEARCH_EVENT_INDEX_NAME =
            ConfigOptions.key("elasticsearch.event.index").stringType().defaultValue("events-");

    public static final ConfigOption<Integer> ELASTICSEARCH_REST_REQUEST_TIMEOUT =
            ConfigOptions.key("elasticsearch.rest.request.timeout").intType().defaultValue(60000);

    public static final ConfigOption<Integer> ELASTICSEARCH_REST_CONNECTION_TIMEOUT =
            ConfigOptions.key("elasticsearch.rest.connect.timeout").intType().defaultValue(30000);

    public static final ConfigOption<Integer> ELASTICSEARCH_REST_SOCKET_TIMEOUT =
            ConfigOptions.key("elasticsearch.rest.socket.timeout").intType().defaultValue(60000);

    public static final ConfigOption<Integer> ELASTICSEARCH_ALERT_MAX_BULK_FLUSH_ACTION =
            ConfigOptions.key("elasticsearch.alert.bulkflushmaxactions").intType().defaultValue(1);

    public static final ConfigOption<Integer> ELASTICSEARCH_EVENT_MAX_BULK_FLUSH_ACTION =
            ConfigOptions.key("elasticsearch.event.bulkflushmaxactions").intType().defaultValue(1);

    public static final ConfigOption<Integer> ELASTICSEARCH_ALERT_STREAM_PARALLELISM =
            ConfigOptions.key("elasticsearch.alert.streamparallelism").intType().noDefaultValue();

    public static final ConfigOption<Integer> ELASTICSEARCH_EVENT_STREAM_PARALLELISM =
            ConfigOptions.key("elasticsearch.event.streamparallelism").intType().noDefaultValue();

    //####################################################################################
    //#======================== Sigma rules specific options ============================#
    //####################################################################################

    public static final ConfigOption<String> SIGMA_RULES_FOLDER_PATH =
            ConfigOptions.key("sigma.rules.folder").stringType().defaultValue("rules/");

    public static final ConfigOption<String> SIGMA_GLOBAL_MAP_PATH =
            ConfigOptions.key("sigma.global.map").stringType().noDefaultValue();

    public static final ConfigOption<String> SIGMA_MAPS_FOLDER_PATH =
            ConfigOptions.key("sigma.maps.folder").stringType().defaultValue("mapping/sigma-maps/");

    //####################################################################################
    //#============================ MISP specific options ===============================#
    //####################################################################################

    public static final ConfigOption<Boolean> MISP_ENRICHMENT_ENABLED =
            ConfigOptions.key("misp.enrichment.enable").booleanType().defaultValue(false);

    public static final ConfigOption<String> MISP_HOST =
            ConfigOptions.key("misp.host").stringType().noDefaultValue();

    public static final ConfigOption<String> MISP_PROTO =
            ConfigOptions.key("misp.proto").stringType().defaultValue("https");

    public static final ConfigOption<String> MISP_AUTOMATION_API_KEY =
            ConfigOptions.key("misp.automation.api.key").stringType().noDefaultValue();

    public static final ConfigOption<String> MISP_MAP_PATH =
            ConfigOptions.key("misp.map").stringType().defaultValue("mapping/misp-map/event_attributes_map.json");

    public static final ConfigOption<String> MISP_MAP_VALUES =
            ConfigOptions.key("misp.map.values").stringType().noDefaultValue();

    //####################################################################################

    private static Configuration nybbleConfiguration = new Configuration();

    public static void setNybbleConfiguration() {

        NybbleAnalyticsConfiguration nybbleProperties = new NybbleAnalyticsConfiguration();

        // Redis Specific
        nybbleConfiguration.setString(REDIS_SERVER_HOST, nybbleProperties.getRedisServerHost());
        nybbleConfiguration.setInteger(REDIS_SERVER_PORT, nybbleProperties.getRedisServerPort());
        nybbleConfiguration.setInteger(REDIS_SERVER_CONNECTION_TIMEOUT, nybbleProperties.getRedisConnectionTimeOut());
        nybbleConfiguration.setLong(REDIS_MISP_CACHE_KEY_EXPIRATION, nybbleProperties.getRedisMispKeyExpire());
        nybbleConfiguration.setLong(REDIS_DNS_CACHE_KEY_EXPIRATION, nybbleProperties.getRedisDnsKeyExpire());
        nybbleConfiguration.setInteger(REDIS_MISP_CACHE_DB_ID, nybbleProperties.getRedisMispCacheId());
        nybbleConfiguration.setInteger(REDIS_DNS_CACHE_DB_ID, nybbleProperties.getRedisDnsCacheId());
        nybbleConfiguration.setInteger(REDIS_IO_THREADS_NUM, nybbleProperties.getRedisIoThreads());
        nybbleConfiguration.setInteger(REDIS_COMPUTE_THREADS_NUM, nybbleProperties.getRedisComputeThreads());

        // Kafka Specific
        nybbleConfiguration.setString(KAFKA_BOOTSTRAP_SERVERS, nybbleProperties.getKafkaBootstrapServers());
        nybbleConfiguration.setString(KAFKA_GROUP_ID, nybbleProperties.getKafkaGroupId());
        nybbleConfiguration.setString(KAFKA_SECURITY_PROTOCOL, nybbleProperties.getKafkaSecurityProtocol());
        nybbleConfiguration.setString(KAFKA_SSL_ENABLED_PROTOCOL, nybbleProperties.getKafkaEnabledSslProtocol());
        nybbleConfiguration.setString(KAFKA_TRUSTSTORE_LOCATION, nybbleProperties.getKafkaTrustStoreLocation());
        nybbleConfiguration.setString(KAFKA_TRUSTSTORE_PASSWORD, nybbleProperties.getKafkaTrustStorePassword());
        nybbleConfiguration.setString(KAFKA_KEYSTORE_LOCATION, nybbleProperties.getKafkaKeyStoreLocation());
        nybbleConfiguration.setString(KAFKA_KEYSTORE_PASSWORD, nybbleProperties.getKafkaKeyStorePassword());
        nybbleConfiguration.setString(KAFKA_KEY_PASSWORD, nybbleProperties.getKafkaKeyPassword());
        nybbleConfiguration.setString(KAFKA_TOPIC_NAME_LIST, nybbleProperties.getKafkaTopicsName());
        nybbleConfiguration.setString(KAFKA_TOPIC_NAME_REGEX, nybbleProperties.getKafkaTopicsPattern());
        nybbleConfiguration.setString(KAFKA_START_POSITION, nybbleProperties.getKafkaStartPosition());
        nybbleConfiguration.setLong(KAFKA_START_EPOCH_TIMESTAMP, nybbleProperties.getKafkaStartEpochTimestamp());

        // Elasticsearch Specific
        nybbleConfiguration.setString(ELASTICSEARCH_HOST, nybbleProperties.getElasticsearchHost());
        nybbleConfiguration.setInteger(ELASTICSEARCH_PORT, nybbleProperties.getElasticsearchPort());
        nybbleConfiguration.setString(ELASTICSEARCH_PROTO, nybbleProperties.getElasticsearchProtocol());
        nybbleConfiguration.setBoolean(ELASTICSEARCH_SSL_ENABLE, nybbleProperties.getElasticsearchSslFlag());
        nybbleConfiguration.setString(ELASTICSEARCH_TRUSTSTORE, nybbleProperties.getElasticsearchTruststoreLocation());
        nybbleConfiguration.setString(ELASTICSEARCH_TRUSTSTORE_PASSWORD, nybbleProperties.getElasticsearchTruststorePassword());
        nybbleConfiguration.setBoolean(ELASTICSEARCH_AUTH_ENABLE, nybbleProperties.getElasticsearchAuthFlag());
        nybbleConfiguration.setString(ELASTICSEARCH_USERNAME, nybbleProperties.getElasticsearchUsername());
        nybbleConfiguration.setString(ELASTICSEARCH_PASSWORD, nybbleProperties.getElasticsearchPassword());
        nybbleConfiguration.setString(ELASTICSEARCH_ALERT_INDEX_NAME, nybbleProperties.getElasticsearchAlertIndex());
        nybbleConfiguration.setString(ELASTICSEARCH_EVENT_INDEX_NAME, nybbleProperties.getElasticsearchEventIndex());
        nybbleConfiguration.setInteger(ELASTICSEARCH_REST_CONNECTION_TIMEOUT, nybbleProperties.getElasticsearchRestConTimeOut());
        nybbleConfiguration.setInteger(ELASTICSEARCH_REST_REQUEST_TIMEOUT, nybbleProperties.getElasticsearchRestReqTimeOut());
        nybbleConfiguration.setInteger(ELASTICSEARCH_REST_SOCKET_TIMEOUT, nybbleProperties.getElasticsearchRestSckTimeOut());
        nybbleConfiguration.setInteger(ELASTICSEARCH_ALERT_MAX_BULK_FLUSH_ACTION, nybbleProperties.getElasticsearchAlertBulkFlushMaxActions());
        nybbleConfiguration.setInteger(ELASTICSEARCH_EVENT_MAX_BULK_FLUSH_ACTION, nybbleProperties.getElasticsearchEventBulkFlushMaxActions());
        nybbleConfiguration.setInteger(ELASTICSEARCH_ALERT_STREAM_PARALLELISM, nybbleProperties.getElasticsearchAlertStreamParallelism());
        nybbleConfiguration.setInteger(ELASTICSEARCH_EVENT_STREAM_PARALLELISM, nybbleProperties.getElasticsearchEventStreamParallelism());

        // Sigma Specific
        nybbleConfiguration.setString(SIGMA_RULES_FOLDER_PATH, nybbleProperties.getSigmaRulesFolder());
        nybbleConfiguration.setString(SIGMA_GLOBAL_MAP_PATH, nybbleProperties.getSigmaGlobalMapFile());
        nybbleConfiguration.setString(SIGMA_MAPS_FOLDER_PATH, nybbleProperties.getSigmaMapsFolder());

        // MISP Specific
        nybbleConfiguration.setBoolean(MISP_ENRICHMENT_ENABLED, nybbleProperties.getMispEnrichmentFlag());
        nybbleConfiguration.setString(MISP_HOST, nybbleProperties.getMispHost());
        nybbleConfiguration.setString(MISP_PROTO, nybbleProperties.getMispProto());
        nybbleConfiguration.setString(MISP_AUTOMATION_API_KEY, nybbleProperties.getMispAutomationKey());
        nybbleConfiguration.setString(MISP_MAP_PATH, nybbleProperties.getMispMapFile());
    }

    public static void setMispMapValues(String mispMapJsonString) {
        nybbleConfiguration.setString(MISP_MAP_VALUES, mispMapJsonString);
    }

    public static Configuration getNybbleConfiguration() {
        return nybbleConfiguration;
    }
}
