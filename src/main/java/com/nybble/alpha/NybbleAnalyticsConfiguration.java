package com.nybble.alpha;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.util.List;

public class NybbleAnalyticsConfiguration {

    private String bootstrapServersIp;
    private String bootstrapServersPort;
    private String kafkaGroupId;
    private List<String> topicsName;
    private String topicsPattern;
    private String startPosition;
    private Long startEpochTimestamp;
    private String redisServerIp;
    private Integer redisServerPort;
    private Integer redisConnectionTimeOut;
    private Long redisKeyExpire;
    private Integer redisIoThreads;
    private Integer redisComputeThreads;
    private Integer redisPoolMaxWait;
    private Boolean redisPoolTestOnBorrow;
    private Boolean redisPoolTestOnReturn;
    private Boolean redisPoolTestWhileIdle;
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
    private String globalMapFile;
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
                                    .setFile(new File("./src/main/resources/config/config.properties"))
                                    .setListDelimiterHandler(new DefaultListDelimiterHandler(',')));

            Configuration nybbleAnalyticsConf = configurationBuilder.getConfiguration();

            // Kafka parameters
            this.bootstrapServersIp = nybbleAnalyticsConf.getString("bootstrap.servers.ip");
            this.bootstrapServersPort = nybbleAnalyticsConf.getString("bootstrap.servers.port");
            this.kafkaGroupId = nybbleAnalyticsConf.getString("group.id");

            if (nybbleAnalyticsConf.getString("topic.name.list") != null) {
                this.topicsName = nybbleAnalyticsConf.getList(String.class, "topic.name.list");
            } else {
                this.topicsName = null;
            }

            if (nybbleAnalyticsConf.getString("topic.name.regex") != null) {
                this.topicsPattern = nybbleAnalyticsConf.getString("topic.name.regex");
            } else {
                this.topicsPattern = null;
            }

            this.startPosition = nybbleAnalyticsConf.getString("start.position");

            if (nybbleAnalyticsConf.getString("start.timestamp") != null) {
                this.startEpochTimestamp = Long.parseLong(nybbleAnalyticsConf.getString("start.timestamp"));
            } else {
                this.startEpochTimestamp = null;
            }

            // Redis parameters
            this.redisServerIp = nybbleAnalyticsConf.getString("redis.server.host");
            this.redisServerPort = nybbleAnalyticsConf.getInt("redis.server.port");
            this.redisConnectionTimeOut = nybbleAnalyticsConf.getInt("redis.server.connection.timeout");
            this.redisKeyExpire = nybbleAnalyticsConf.getLong("redis.key.expire");
            this.redisIoThreads = nybbleAnalyticsConf.getInt("redis.io.threads");
            this.redisComputeThreads = nybbleAnalyticsConf.getInt("redis.compute.threads");
            this.redisPoolMaxWait = nybbleAnalyticsConf.getInt("redis.pool.maxwait");
            this.redisPoolTestOnBorrow = nybbleAnalyticsConf.getBoolean("redis.pool.testonborrow");
            this.redisPoolTestOnReturn = nybbleAnalyticsConf.getBoolean("redis.pool.testonreturn");
            this.redisPoolTestWhileIdle = nybbleAnalyticsConf.getBoolean("redis.pool.testwhileidle");

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
            this.globalMapFile = nybbleAnalyticsConf.getString("global.map");
            this.sigmaRulesFolder = nybbleAnalyticsConf.getString("rules.folder");
            this.sigmaMapsFolder = nybbleAnalyticsConf.getString("maps.folder");

            // MISP parameters
            this.mispEnrichmentFlag = nybbleAnalyticsConf.getBoolean("misp.enrichment");

            this.mispHost = nybbleAnalyticsConf.getString("misp.host");

            if (nybbleAnalyticsConf.getString("misp.ssl.enable").equals("true")) {
                this.mispProto = "https";
            } else {
                this.mispProto = "http";
            }

            this.mispAutomationKey = nybbleAnalyticsConf.getString("misp.automation.key");
            this.mispMapFile = nybbleAnalyticsConf.getString("misp.map");


        } catch (ConfigurationException cex) {
            cex.printStackTrace();
        }
    }

    public String getKafkaBootstrapServersIp() { return this.bootstrapServersIp; }

    public String getKafkaBootstrapServersPort() { return this.bootstrapServersPort; }

    public String getKafkaBootstrapServers() { return this.bootstrapServersIp + ":" + this.bootstrapServersPort; }

    public String getKafkaGroupId() { return this.kafkaGroupId; }

    public List<String> getKafkaTopicsName() { return this.topicsName; }

    public String getKafkaTopicsPattern() { return this.topicsPattern; }

    public String getStartPosition() { return startPosition; }

    public Long getStartEpochTimestamp() { return this.startEpochTimestamp; }

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

    public Long getRedisKeyExpire() { return redisKeyExpire; }

    public Integer getRedisIoThreads() { return redisIoThreads; }

    public Integer getRedisComputeThreads() { return redisComputeThreads; }

    public Integer getRedisPoolMaxWait() { return redisPoolMaxWait; }

    public Boolean getRedisPoolTestOnBorrow() { return redisPoolTestOnBorrow; }

    public Boolean getRedisPoolTestOnReturn() { return redisPoolTestOnReturn; }

    public Boolean getRedisPoolTestWhileIdle() { return redisPoolTestWhileIdle; }

    public String getGlobalMapFile() { return globalMapFile; }

    public String getSigmaMapsFolder() { return sigmaMapsFolder; }

    public String getSigmaRulesFolder() { return sigmaRulesFolder; }

    public Boolean getMispEnrichmentFlag() { return mispEnrichmentFlag; }

    public String getMispHost() { return mispHost; }

    public String getMispProto() { return mispProto; }

    public String getMispAutomationKey() { return mispAutomationKey; }

    public String getMispMapFile() { return mispMapFile; }
}