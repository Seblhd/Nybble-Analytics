package com.nybble.alpha;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class NybbleAnalyticsConfiguration {

    private String bootstrapServersIp;
    private String bootstrapServersPort;
    private String kafkaGroupId;
    private String topicName;
    private String elasticsearchHost;
    private Integer elasticsearchPort;
    private String elasticsearchProto;
    private String elasticsearchEventIndex;
    private String elasticsearchAlertIndex;
    private String globalMapFile;
    private String sigmaRulesFolder;
    private String sigmaMapsFolder;

    public NybbleAnalyticsConfiguration() {
        try (InputStream configFile = new FileInputStream("./src/main/resources/config/config.properties")) {

            Properties nybbleAnalyticsConf = new Properties();

            // Load a properties file
            nybbleAnalyticsConf.load(configFile);

            // Kafka parameters
            this.bootstrapServersIp = nybbleAnalyticsConf.getProperty("bootstrap.servers.ip");
            this.bootstrapServersPort = nybbleAnalyticsConf.getProperty("bootstrap.servers.port");
            this.kafkaGroupId = nybbleAnalyticsConf.getProperty("group.id");
            this.topicName = nybbleAnalyticsConf.getProperty("topic.name");

            // Elasticsearch parameters
            this.elasticsearchHost = nybbleAnalyticsConf.getProperty("elasticsearch.host");
            this.elasticsearchPort = Integer.parseInt(nybbleAnalyticsConf.getProperty("elasticsearch.port"));
            this.elasticsearchProto = nybbleAnalyticsConf.getProperty("elasticsearch.proto");
            this.elasticsearchEventIndex = nybbleAnalyticsConf.getProperty("elasticsearch.event.index");
            this.elasticsearchAlertIndex = nybbleAnalyticsConf.getProperty("elasticsearch.alert.index");

            // Sigma Rules parameters
            this.globalMapFile = nybbleAnalyticsConf.getProperty("global.map");
            this.sigmaRulesFolder = nybbleAnalyticsConf.getProperty("rules.folder");
            this.sigmaMapsFolder = nybbleAnalyticsConf.getProperty("maps.folder");

        } catch (IOException e) {
            e.printStackTrace();
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

    public String getKafkaTopicName() {
        return this.topicName;
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
