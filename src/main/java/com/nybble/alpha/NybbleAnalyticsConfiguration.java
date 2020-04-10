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
    private String elasticsearchHost;
    private Integer elasticsearchPort;
    private String elasticsearchProto;
    private String elasticsearchEventIndex;
    private String elasticsearchAlertIndex;
    private String globalMapFile;
    private String sigmaRulesFolder;
    private String sigmaMapsFolder;

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

            // Elasticsearch parameters
            this.elasticsearchHost = nybbleAnalyticsConf.getString("elasticsearch.host");
            this.elasticsearchPort = nybbleAnalyticsConf.getInt("elasticsearch.port");
            this.elasticsearchProto = nybbleAnalyticsConf.getString("elasticsearch.proto");
            this.elasticsearchEventIndex = nybbleAnalyticsConf.getString("elasticsearch.event.index");
            this.elasticsearchAlertIndex = nybbleAnalyticsConf.getString("elasticsearch.alert.index");

            // Sigma Rules parameters
            this.globalMapFile = nybbleAnalyticsConf.getString("global.map");
            this.sigmaRulesFolder = nybbleAnalyticsConf.getString("rules.folder");
            this.sigmaMapsFolder = nybbleAnalyticsConf.getString("maps.folder");

        } catch (ConfigurationException cex) {
            cex.printStackTrace();
        }
    }

    public String getKafkaBootstrapServersIp() {
        return this.bootstrapServersIp;
    }

    public String getKafkaBootstrapServersPort() {
        return this.bootstrapServersPort;
    }

    public String getKafkaBootstrapServers() {
        return this.bootstrapServersIp + ":" + this.bootstrapServersPort;
    }

    public String getKafkaGroupId() {
        return this.kafkaGroupId;
    }

    public List<String> getKafkaTopicsName() {
        return this.topicsName;
    }

    public String getKafkaTopicsPattern() {
        return this.topicsPattern;
    }

    public String getStartPosition() {
        return startPosition;
    }

    public Long getStartEpochTimestamp() {
        return this.startEpochTimestamp;
    }

    public String getElasticsearchHost() { return elasticsearchHost; }

    public Integer getElasticsearchPort() {
        return elasticsearchPort;
    }

    public String getElasticsearchProto() { return elasticsearchProto; }

    public String getElasticsearchEventIndex() { return elasticsearchEventIndex; }

    public String getElasticsearchAlertIndex() {
        return elasticsearchAlertIndex;
    }

    public String getGlobalMapFile() { return globalMapFile; }

    public String getSigmaMapsFolder() { return sigmaMapsFolder; }

    public String getSigmaRulesFolder() { return sigmaRulesFolder; }
}
