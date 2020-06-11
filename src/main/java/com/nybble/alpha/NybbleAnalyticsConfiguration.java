package com.nybble.alpha;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;

public class NybbleAnalyticsConfiguration {

    private String kafkaBootstrapServers;
    private String kafkaGroupId;
    private String kafkaTopicsName;
    private String kafkaTopicsPattern;
    private String kafkaStartPosition;
    private Long kafkaStartEpochTimestamp;
    private String redisServerIp;
    private Integer redisServerPort;
    private Integer redisConnectionTimeOut;
    private Long redisMispKeyExpire;
    private Long redisDnsKeyExpire;
    private Integer redisIoThreads;
    private Integer redisComputeThreads;
    private Integer redisMispCacheId;
    private Integer redisDnsCacheId;
    private String elasticsearchHost;
    private Integer elasticsearchPort;
    private String elasticsearchProto;
    private String elasticsearchEventIndex;
    private String elasticsearchAlertIndex;
    private Integer elasticsearchRestReqTimeOut;
    private Integer elasticsearchRestConTimeOut;
    private Integer elasticsearchRestSckTimeOut;
    private Integer elasticsearchEventBulkFlushMaxActions;
    private Integer elasticsearchAlertBulkFlushMaxActions;
    private Integer elasticsearchEventStreamParallelism;
    private Integer elasticsearchAlertStreamParallelism;
    private String sigmaGlobalMapFile;
    private String sigmaRulesFolder;
    private String sigmaMapsFolder;
    private Boolean mispEnrichmentFlag;
    private String mispHost;
    private String mispProto;
    private String mispAutomationKey;
    private String mispMapFile;

    public NybbleAnalyticsConfiguration() {


        try {

            Parameters configBuilderParams = new Parameters();

            FileBasedConfigurationBuilder<FileBasedConfiguration> configurationBuilder =
                    new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                            .configure(configBuilderParams.properties()
                                    .setFile(new File("./src/main/resources/config/config.properties")));
                                    //.setListDelimiterHandler(new DefaultListDelimiterHandler(',')));

            Configuration nybbleAnalyticsConf = configurationBuilder.getConfiguration();

            // Kafka parameters
            this.kafkaBootstrapServers = nybbleAnalyticsConf.getString("kafka.bootstrap.servers");
            this.kafkaGroupId = nybbleAnalyticsConf.getString("kafka.group.id");

            if (nybbleAnalyticsConf.getString("kafka.topic.name.list") != null) {
                this.kafkaTopicsName = nybbleAnalyticsConf.getString("kafka.topic.name.list");
            } else {
                this.kafkaTopicsName = "";
            }

            if (nybbleAnalyticsConf.getString("kafka.topic.name.regex") != null) {
                this.kafkaTopicsPattern = nybbleAnalyticsConf.getString("kafka.topic.name.regex");
            } else {
                this.kafkaTopicsPattern = "";
            }

            this.kafkaStartPosition = nybbleAnalyticsConf.getString("kafka.start.position");

            if (nybbleAnalyticsConf.getString("kafka.start.timestamp") != null) {
                this.kafkaStartEpochTimestamp = Long.parseLong(nybbleAnalyticsConf.getString("kafka.start.timestamp"));
            } else {
                this.kafkaStartEpochTimestamp = 0L;
            }

            // Redis parameters
            this.redisServerIp = nybbleAnalyticsConf.getString("redis.server.host");
            this.redisServerPort = nybbleAnalyticsConf.getInt("redis.server.port");
            this.redisConnectionTimeOut = nybbleAnalyticsConf.getInt("redis.server.connection.timeout");
            this.redisMispKeyExpire = nybbleAnalyticsConf.getLong("redis.misp.key.expire");
            this.redisDnsKeyExpire = nybbleAnalyticsConf.getLong("redis.dns.key.expire");
            this.redisIoThreads = nybbleAnalyticsConf.getInt("redis.io.threads");
            this.redisComputeThreads = nybbleAnalyticsConf.getInt("redis.compute.threads");
            this.redisMispCacheId = nybbleAnalyticsConf.getInt("redis.misp.cache.id");
            this.redisDnsCacheId = nybbleAnalyticsConf.getInt("redis.dns.cache.id");

            // Elasticsearch parameters
            this.elasticsearchHost = nybbleAnalyticsConf.getString("elasticsearch.host");
            this.elasticsearchPort = nybbleAnalyticsConf.getInt("elasticsearch.port");
            this.elasticsearchProto = nybbleAnalyticsConf.getString("elasticsearch.proto");
            this.elasticsearchEventIndex = nybbleAnalyticsConf.getString("elasticsearch.event.index");
            this.elasticsearchAlertIndex = nybbleAnalyticsConf.getString("elasticsearch.alert.index");
            this.elasticsearchRestReqTimeOut = nybbleAnalyticsConf.getInt("elasticsearch.rest.request.timeout");
            this.elasticsearchRestConTimeOut = nybbleAnalyticsConf.getInt("elasticsearch.rest.connect.timeout");
            this.elasticsearchRestSckTimeOut = nybbleAnalyticsConf.getInt("elasticsearch.rest.socket.timeout");
            this.elasticsearchAlertBulkFlushMaxActions = nybbleAnalyticsConf.getInt("elasticsearch.alert.bulkflushmaxactions");
            this.elasticsearchEventBulkFlushMaxActions = nybbleAnalyticsConf.getInt("elasticsearch.event.bulkflushmaxactions");
            this.elasticsearchAlertStreamParallelism = nybbleAnalyticsConf.getInt("elasticsearch.alert.streamparallelism");
            this.elasticsearchEventStreamParallelism = nybbleAnalyticsConf.getInt("elasticsearch.event.streamparallelism");

            // Sigma Rules parameters
            this.sigmaGlobalMapFile = System.getenv("NYBBLE_HOME") + nybbleAnalyticsConf.getString("sigma.global.map");
            this.sigmaRulesFolder = System.getenv("NYBBLE_HOME") + nybbleAnalyticsConf.getString("sigma.rules.folder");
            this.sigmaMapsFolder = System.getenv("NYBBLE_HOME") + nybbleAnalyticsConf.getString("sigma.maps.folder");

            // MISP parameters
            this.mispEnrichmentFlag = nybbleAnalyticsConf.getBoolean("misp.enrichment.enable");

            this.mispHost = nybbleAnalyticsConf.getString("misp.host");

            if (nybbleAnalyticsConf.getString("misp.ssl.enable").equals("true")) {
                this.mispProto = "https";
            } else {
                this.mispProto = "http";
            }

            this.mispAutomationKey = nybbleAnalyticsConf.getString("misp.automation.key");
            this.mispMapFile = System.getenv("NYBBLE_HOME") + nybbleAnalyticsConf.getString("misp.map");


        } catch (ConfigurationException cex) {
            cex.printStackTrace();
        }
    }

    public String getKafkaBootstrapServers() { return this.kafkaBootstrapServers; }

    public String getKafkaGroupId() { return this.kafkaGroupId; }

    public String getKafkaTopicsName() { return this.kafkaTopicsName; }

    public String getKafkaTopicsPattern() { return this.kafkaTopicsPattern; }

    public String getKafkaStartPosition() { return kafkaStartPosition; }

    public Long getKafkaStartEpochTimestamp() { return this.kafkaStartEpochTimestamp; }

    public String getElasticsearchHost() { return elasticsearchHost; }

    public Integer getElasticsearchPort() { return elasticsearchPort; }

    public String getElasticsearchProto() { return elasticsearchProto; }

    public String getElasticsearchEventIndex() { return elasticsearchEventIndex; }

    public String getElasticsearchAlertIndex() { return elasticsearchAlertIndex; }

    public Integer getElasticsearchRestReqTimeOut() { return elasticsearchRestReqTimeOut; }

    public Integer getElasticsearchRestConTimeOut() { return elasticsearchRestConTimeOut; }

    public Integer getElasticsearchRestSckTimeOut() { return elasticsearchRestSckTimeOut; }

    public Integer getElasticsearchAlertBulkFlushMaxActions() { return elasticsearchAlertBulkFlushMaxActions; }

    public Integer getElasticsearchEventBulkFlushMaxActions() { return elasticsearchEventBulkFlushMaxActions; }

    public Integer getElasticsearchAlertStreamParallelism() { return elasticsearchAlertStreamParallelism; }

    public Integer getElasticsearchEventStreamParallelism() { return elasticsearchEventStreamParallelism; }

    public String getRedisServerHost() { return redisServerIp; }

    public Integer getRedisServerPort() { return redisServerPort; }

    public Integer getRedisConnectionTimeOut() { return redisConnectionTimeOut; }

    public Long getRedisMispKeyExpire() { return redisMispKeyExpire; }

    public Long getRedisDnsKeyExpire() { return redisDnsKeyExpire; }

    public Integer getRedisIoThreads() { return redisIoThreads; }

    public Integer getRedisComputeThreads() { return redisComputeThreads; }

    public Integer getRedisMispCacheId() { return redisMispCacheId; }

    public Integer getRedisDnsCacheId() { return redisDnsCacheId; }

    public String getSigmaGlobalMapFile() { return sigmaGlobalMapFile; }

    public String getSigmaMapsFolder() { return sigmaMapsFolder; }

    public String getSigmaRulesFolder() { return sigmaRulesFolder; }

    public Boolean getMispEnrichmentFlag() { return mispEnrichmentFlag; }

    public String getMispHost() { return mispHost; }

    public String getMispProto() { return mispProto; }

    public String getMispAutomationKey() { return mispAutomationKey; }

    public String getMispMapFile() { return mispMapFile; }
}