package com.nybble.alpha.event_enrichment;

import com.nybble.alpha.NybbleAnalyticsConfiguration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MispEnrichement {

    private String mispURL;
    private String mispAutomationKey;
    private static ObjectMapper jsonMapper = new ObjectMapper();
    private ObjectNode mispAttributesNode = jsonMapper.createObjectNode();
    private ObjectNode httpDataNode = jsonMapper.createObjectNode();
    private CloseableHttpClient mispClient = HttpClients.createDefault();
    // Get configuration from config file.
    private NybbleAnalyticsConfiguration nybbleAnalyticsConfiguration = new NybbleAnalyticsConfiguration();
    private HashMap<String, ArrayList<Tuple2<String, String>>> mispMappingMap;

    public MispEnrichement() {

        // Build MISP URL with information from config.properties
        String mispHost = nybbleAnalyticsConfiguration.getMispHost();
        String mispProtocol = nybbleAnalyticsConfiguration.getMispProto();
        String mispGetAttributes = "/attributes/restSearch";
        this.mispURL = mispProtocol +"://"+ mispHost + mispGetAttributes;
        this.mispAutomationKey = nybbleAnalyticsConfiguration.getMispAutomationKey();
    }

    public ObjectNode getAttributes(String mispEventTags, String mispAttributeType, String mispAttributeValue) throws IOException {

        // Reset node containing MISP Request result.
        mispAttributesNode.removeAll();
        // Reset HashMap containing data for MISP POST.
        httpDataNode.removeAll();

        httpDataNode.put("returnFormat", "json");
        httpDataNode.put("tags", "TOR-node");
        httpDataNode.put("type", "ip-dst");
        httpDataNode.put("value", "98.251.10.2400");

        HttpUriRequest mispHttpRequest = RequestBuilder.post()
                .setUri(mispURL)
                .setHeader(HttpHeaders.AUTHORIZATION, mispAutomationKey)
                .setHeader(HttpHeaders.ACCEPT, "application/json")
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .setEntity(new StringEntity(jsonMapper.writeValueAsString(httpDataNode)))
                .build();

        CloseableHttpResponse mispResponse = mispClient.execute(mispHttpRequest);

        System.out.println(EntityUtils.toString(mispResponse.getEntity()));

        return mispAttributesNode;
    }

    public void setMispMapping() throws IOException {
        // Create a ObjectNode containing information in MispMap JSON file.
        ObjectNode mispMapNode = jsonMapper.readValue(new File(nybbleAnalyticsConfiguration.getMispMapFile()), ObjectNode.class);

        // Iterate on "Tags" to retrieve all MISP tags in mapping file
        Iterator<Map.Entry<String, JsonNode>> mispMapIterator =  mispMapNode.get("tags").fields();

        while (mispMapIterator.hasNext()) {

            // Get value of next MISP Tag
            Map.Entry<String, JsonNode> mispTagMap = mispMapIterator.next();

            System.out.println("Current MISP Tag key : " + mispTagMap.getKey());
            System.out.println("Current MISP Tag value : " + mispTagMap.getValue().toString());
        }
    }
}
