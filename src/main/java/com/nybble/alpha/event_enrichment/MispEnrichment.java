package com.nybble.alpha.event_enrichment;

import com.nybble.alpha.NybbleFlinkConfiguration;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHeaders;
import org.apache.log4j.Logger;
import org.asynchttpclient.AsyncHttpClient;
import static org.asynchttpclient.Dsl.*;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

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
    private Configuration nybbleFlinkConfiguration = NybbleFlinkConfiguration.getNybbleConfiguration();
    private URI mispURL = URI.create(nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.MISP_PROTO) +
            "://" + nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.MISP_HOST) + "/attributes/restSearch");
    private String mispAutomationKey = nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.MISP_AUTOMATION_API_KEY);
    private AsyncHttpClient mispAsyncRestClient = asyncHttpClient(config().setMaxConnectionsPerHost(16).setConnectionPoolCleanerPeriod(3000));
    private HashMap<String, ArrayList<Tuple>> mispMappingMap = new HashMap<>();
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

    public void setMispMapping(String mispMapJsonString) throws IOException {

        // Create a ObjectNode containing information in MispMap JSON String.
        ObjectNode mispMapNode = jsonMapper.readValue(mispMapJsonString, ObjectNode.class);

        // Iterate though all source fields to be enrich.
        Iterator<Map.Entry<String, JsonNode>> mispMapIterator = mispMapNode.fields();

        while (mispMapIterator.hasNext()) {

            Map.Entry<String, JsonNode> srcFieldEnrich = mispMapIterator.next();

            // Add all source fields from mapping file. This list wil be use to check which fields
            // from events will be used for enrichment.
            if (!srcEnrichmentFieldArray.contains(srcFieldEnrich.getKey())) {
                srcEnrichmentFieldArray.add(srcFieldEnrich.getKey());
            }

            // For each source field, Iterate to retrieve all MISP Tags for enrichment.
            Iterator<Map.Entry<String, JsonNode>> mispTagsIterator = srcFieldEnrich.getValue().fields();

            for(int x = 0; x < srcFieldEnrich.getValue().size(); x++) {

                Map.Entry<String, JsonNode> mispTagMap = mispTagsIterator.next();

                for (int y = 0; y < mispTagMap.getValue().size(); y++) {

                    ObjectNode currentMispNode = mispTagMap.getValue().get(y).deepCopy();

                    // Create entry in mispMappingMap HashMap to know which tags, type and value from event field to use for MISP request.
                    // Each ECS field is associated with one or more MISP event tag, MISP attribute type and option(s) specific to type.
                    if (currentMispNode.get("type").asText().equals("ip")) {
                        if (currentMispNode.has("mispAttribute") &&
                                currentMispNode.has("type") &&
                                currentMispNode.has("enrichPublicOnly")) {

                            // Create a new Tuple4 where :
                            // f0 -> MISP Event tag
                            // f1 -> MISP Attribute Type.
                            // f2 -> Global type
                            // f3 -> Enrichment option
                            Tuple4<String, String, String, Boolean> mispEnrichIpTuple = new Tuple4<>();
                            mispEnrichIpTuple.setFields(mispTagMap.getKey(),
                                    currentMispNode.get("mispAttribute").asText(),
                                    currentMispNode.get("type").asText(),
                                    currentMispNode.get("enrichPublicOnly").asBoolean());

                            buildMispHashMap(srcFieldEnrich.getKey(), mispEnrichIpTuple);
                        }
                    } else if (currentMispNode.get("type").asText().equals("domain")) {
                        if (currentMispNode.has("mispAttribute") &&
                                currentMispNode.has("type") &&
                                currentMispNode.has("resolveName")) {

                            // Create a new Tuple4 where :
                            // f0 -> MISP Event tag
                            // f1 -> MISP Attribute Type.
                            // f2 -> Global type
                            // f3 -> Enrichment option
                            Tuple4<String, String, String, Boolean> mispEnrichDomainTuple = new Tuple4<>();
                            mispEnrichDomainTuple.setFields(mispTagMap.getKey(),
                                    currentMispNode.get("mispAttribute").asText(),
                                    currentMispNode.get("type").asText(),
                                    currentMispNode.get("resolveName").asBoolean());

                            buildMispHashMap(srcFieldEnrich.getKey(), mispEnrichDomainTuple);
                        }
                    } else {
                        enrichmentEngineLogger.error("MISP Mapping : type \"" + mispTagMap.getValue().get("type").asText() + "\" is not supported");
                    }
                }
            }
        }

        // When MISP Mapping file as been processed, set values for Flink Map Function.
        EnrichableFieldsFinder.setSrcEnrichmentFieldArray(srcEnrichmentFieldArray);
        EnrichableFieldsFinder.setMispMappingMap(mispMappingMap);
    }

    private void buildMispHashMap(String srcField, Tuple mispEnrichTuple) {

        if (mispMappingMap.containsKey(srcField)) {
            // Get the current ArrayList from mismMappingMap and add new tuple if not already existing and update mispMappingMap
            ArrayList<Tuple> mispTupleList = mispMappingMap.get(srcField);
            if (!mispTupleList.contains(mispEnrichTuple)) {
                mispTupleList.add(mispEnrichTuple);
                mispMappingMap.replace(srcField, mispTupleList);
            }

        } else if (!mispMappingMap.containsKey(srcField)) {
            // Create a new ArrayList that will contains all MISP Event tag and Attribute type tuples.
            ArrayList<Tuple> mispTupleList = new ArrayList<>();
            mispTupleList.add(mispEnrichTuple);

            // Add an entry in mispMappingMap HashMap with current ECS field as key and ArrayList of tuples as value.
            mispMappingMap.put(srcField, mispTupleList);
        }
    }
}
