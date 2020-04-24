package com.nybble.alpha.event_enrichment;

import com.nybble.alpha.NybbleAnalyticsConfiguration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;

import java.io.UnsupportedEncodingException;

public class MispEnrichement {

    private String mispURL;
    private String mispAutomationKey;
    private static ObjectMapper jsonMapper = new ObjectMapper();
    private ObjectNode mispAttributesNode = jsonMapper.createObjectNode();

    public MispEnrichement() {

        // Get configuration from config file.
        NybbleAnalyticsConfiguration nybbleAnalyticsConfiguration = new NybbleAnalyticsConfiguration();

        // Build MISP URL with information from config.properties
        String mispHost = nybbleAnalyticsConfiguration.getMispHost();
        String mispProtocol = nybbleAnalyticsConfiguration.getMispProto();
        String mispGetAttributes = "/attributes/restSearch";
        this.mispURL = mispProtocol +"://"+ mispHost + mispGetAttributes;
        this.mispAutomationKey = nybbleAnalyticsConfiguration.getMispAutomationKey();

        System.out.println("MISP Url is " + mispURL);
    }

    public ObjectNode getAttributes(String attribute) throws UnsupportedEncodingException {

        mispAttributesNode.removeAll();



        HttpUriRequest mispHttpRequest = RequestBuilder.post()
                .setUri(mispURL)
                .setHeader(HttpHeaders.AUTHORIZATION, mispAutomationKey)
                .setHeader(HttpHeaders.ACCEPT, "application/json")
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json").setEntity(new StringEntity(""))
                .build();

        return mispAttributesNode;
    }
}
