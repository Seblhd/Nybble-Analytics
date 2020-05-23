package com.nybble.alpha.event_enrichment;

import com.nybble.alpha.NybbleAnalyticsConfiguration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHeaders;
import org.apache.log4j.Logger;
import org.asynchttpclient.AsyncHttpClient;
import static org.asynchttpclient.Dsl.*;

import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MispEnrichment {

    private static ObjectMapper jsonMapper = new ObjectMapper();
    private NybbleAnalyticsConfiguration nybbleAnalyticsConfiguration = new NybbleAnalyticsConfiguration();
    private URI mispURL = URI.create(nybbleAnalyticsConfiguration.getMispProto() + "://" + nybbleAnalyticsConfiguration.getMispHost() + "/attributes/restSearch");
    private String mispAutomationKey = nybbleAnalyticsConfiguration.getMispAutomationKey();
    private AsyncHttpClient mispAsyncRestClient = asyncHttpClient(config().setMaxConnectionsPerHost(64).setConnectionPoolCleanerPeriod(3000));
    private HashMap<String, ArrayList<Tuple2<String, String>>> mispMappingMap = new HashMap<>();
    private ArrayList<String> srcEnrichmentFieldArray = new ArrayList<>();
    private static Logger enrichmentEngineLogger = Logger.getLogger("enrichmentEngineFile");

    public ObjectNode mispRequest(String mispEventTags, String mispAttributeType, String mispAttributeValue) throws JsonProcessingException {

        // Create node containing MISP Request result.
        ObjectNode mispRequestNode;
        // Create node containing data for MISP POST.
        ObjectNode httpDataNode = jsonMapper.createObjectNode();

        httpDataNode.put("returnFormat", "json");
        httpDataNode.put("tags", mispEventTags);
        httpDataNode.put("type", mispAttributeType);
        httpDataNode.put("value", mispAttributeValue);

        Request mispAsyncRestRequest = post(mispURL.toString())
                .setHeader(HttpHeaders.AUTHORIZATION, mispAutomationKey)
                .setHeader(HttpHeaders.ACCEPT, "application/json")
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .setBody(jsonMapper.writeValueAsString(httpDataNode))
                .build();

        try {
            // Get information from MISP
            Future<Response> mispAsyncRestReponse = mispAsyncRestClient.executeRequest(mispAsyncRestRequest);

            // Put JSON formatted answer in mispAttributesNode, this node will be returned for enrichment.
            mispRequestNode = jsonMapper.readTree(mispAsyncRestReponse.get().getResponseBody()).get("response").deepCopy();

        } catch (InterruptedException | ExecutionException | IOException httpHostConnectException) {
            // If connection is refused or timed out, return an empty node.
            enrichmentEngineLogger.error("MISP Response : " + httpHostConnectException);
            mispRequestNode = jsonMapper.createObjectNode();
        }

        return mispRequestNode;
    }

    public void setMispMapping() throws IOException {
        // Create a ObjectNode containing information in MispMap JSON file.
        ObjectNode mispMapNode = jsonMapper.readValue(new File(nybbleAnalyticsConfiguration.getMispMapFile()), ObjectNode.class);

        // Iterate on "Tags" to retrieve all MISP tags in mapping file
        Iterator<Map.Entry<String, JsonNode>> mispMapIterator =  mispMapNode.get("tags").fields();

        while (mispMapIterator.hasNext()) {

            // Get value of next MISP Tag
            Map.Entry<String, JsonNode> mispTagMap = mispMapIterator.next();

            Iterator<Map.Entry<String, JsonNode>> mispTypeIterator = mispTagMap.getValue().fields();

            for (int x = 0; x < mispTagMap.getValue().size(); x++) {

                Map.Entry<String, JsonNode> mispTypeMap = mispTypeIterator.next();

                ArrayNode mispTypeMappingNode = (ArrayNode) jsonMapper.readTree(mispTypeMap.getValue().deepCopy().toString());

                if (mispTypeMappingNode.isArray()) {
                    mispTypeMappingNode.forEach(ecsField -> {

                        // Add all ECS field from mapping file. this list wil be use to check which fields
                        // from events need to be used for enrichment.
                        if (!srcEnrichmentFieldArray.contains(ecsField.asText())) {
                            srcEnrichmentFieldArray.add(ecsField.asText());
                        }

                        // Create entry in mispMappingMap HashMap to know which tags, type and value from event field to use for MISP request.
                        // Each ECS field is associated with one or more MISP event tag and MISP attribute type.

                        // Create a new tuple where f0 is MISP Event tag and f1 MISP Attribute Type.
                        Tuple2<String, String> mispTagTypeTuple = new Tuple2<>();
                        mispTagTypeTuple.setFields(mispTagMap.getKey(), mispTypeMap.getKey());

                        if (mispMappingMap.containsKey(ecsField.asText())) {
                            // Get the current ArrayList from mismMappingMap and add new tuple if not already existing and update mispMappingMap
                            ArrayList<Tuple2<String, String>> mispTupleList = mispMappingMap.get(ecsField.asText());
                            if (!mispTupleList.contains(mispTagTypeTuple)) {
                                mispTupleList.add(mispTagTypeTuple);
                                mispMappingMap.replace(ecsField.asText(), mispTupleList);
                            }

                        } else if (!mispMappingMap.containsKey(ecsField.asText())) {
                            // Create a new ArrayList that will contains all MISP Event tag and Attribute type tuples.
                            ArrayList<Tuple2<String, String>> mispTupleList = new ArrayList<>();
                            mispTupleList.add(mispTagTypeTuple);

                            // Add an entry in mispMappingMap HashMap with current ECS field as key and ArrayList of tuples as value.
                            mispMappingMap.put(ecsField.asText(), mispTupleList);
                        }
                    });
                }
            }
        }

        // When MISP Mapping file as been processed, set values for Flink Map Function.
        EnrichableFieldsFinder.setSrcEnrichmentFieldArray(srcEnrichmentFieldArray);
        EnrichableFieldsFinder.setMispMappingMap(mispMappingMap);
    }
}
