package com.nybble.alpha.event_enrichment;

import com.nybble.alpha.NybbleAnalyticsConfiguration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MipsEnrichment {

    private String mispURL;
    private String mispAutomationKey;
    private static ObjectMapper jsonMapper = new ObjectMapper();
    private ObjectNode mispAttributesNode = jsonMapper.createObjectNode();
    private ObjectNode httpDataNode = jsonMapper.createObjectNode();
    private CloseableHttpClient mispClient = HttpClients.createDefault();
    private NybbleAnalyticsConfiguration nybbleAnalyticsConfiguration = new NybbleAnalyticsConfiguration();
    private HashMap<String, ArrayList<Tuple2<String, String>>> mispMappingMap = new HashMap<>();
    private ArrayList<String> srcEnrichmentFieldArray = new ArrayList<>();
    private static Logger enrichmentEngineLogger = Logger.getLogger("enrichmentEngineFile");

    public MipsEnrichment() {

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
        httpDataNode.put("tags", mispEventTags);
        httpDataNode.put("type", mispAttributeType);
        httpDataNode.put("value", mispAttributeValue);

        HttpUriRequest mispHttpRequest = RequestBuilder.post()
                .setUri(mispURL)
                .setHeader(HttpHeaders.AUTHORIZATION, mispAutomationKey)
                .setHeader(HttpHeaders.ACCEPT, "application/json")
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .setEntity(new StringEntity(jsonMapper.writeValueAsString(httpDataNode)))
                .build();

        try {
            CloseableHttpResponse mispResponse = mispClient.execute(mispHttpRequest);
            mispAttributesNode = jsonMapper.readTree(EntityUtils.toString(mispResponse.getEntity())).get("response").deepCopy();
        } catch (HttpHostConnectException httpHostConnectException) {
            // If connection is refused or timed out, return an empty node.
            enrichmentEngineLogger.error("MISP Response : " + httpHostConnectException);
            ObjectNode emptyAttributeNode = jsonMapper.createObjectNode();
            emptyAttributeNode.putArray("Attribute");

            return emptyAttributeNode;
        }

        return mispAttributesNode;
    }

    public ObjectNode enrichEvent(ObjectNode mispAttributesNode) {

        // Create a Node that will contains information from MISP and then will be added to EventNode.
        ObjectNode mispNode = jsonMapper.createObjectNode();
        // Create an ArrayList that will contains all MISP Event ID where attribute value has been found.
        ArrayList<String> mispEventIDList = new ArrayList<>();
        // Create an ArrayList that will contains all unique MISP Category for the attribute value.
        ArrayList<String> mispAttributeCategoryList = new ArrayList<>();
        // Create an ArrayList that will contains all unique MISP Tags for the attribute value.
        ArrayList<String> mispAttributeTagsList = new ArrayList<>();

        for (int x = 0; x < mispAttributesNode.get("Attribute").size(); x++) {

            // Get each Event from MISP answer.
            ObjectNode mispEvent = mispAttributesNode.get("Attribute").get(x).deepCopy();

            // Add MISP Event ID from current Event.
            if (mispEvent.has("event_id")) {
                mispEventIDList.add(mispEvent.get("event_id").asText());
            }

            // Add MISP Attribute Category from current event.
            if (mispEvent.has("category")) {
                if(!mispAttributeCategoryList.contains(mispEvent.get("category").asText())) {
                    mispAttributeCategoryList.add(mispEvent.get("category").asText());
                }
            }

            // Add MISP Atrribute tags from current event.
            if (mispEvent.has("Tag")) {
                mispEvent.get("Tag").forEach(tag -> {
                    if (!mispAttributeTagsList.contains(tag.get("name").asText())) {
                        mispAttributeTagsList.add(tag.get("name").asText());
                    }
                });
            }
        }

        // If ArrayList is not empty, convert to ArrayNode and then add to final mispNode.
        if (!mispEventIDList.isEmpty()) {
            ArrayNode mispEventIDNode = jsonMapper.valueToTree(mispEventIDList);
            mispNode.putArray("misp.event.id").addAll(mispEventIDNode);
        }

        if (!mispAttributeCategoryList.isEmpty()) {
            ArrayNode mispAttributeCategoryNode = jsonMapper.valueToTree(mispAttributeCategoryList);
            mispNode.putArray("misp.attribute.category").addAll(mispAttributeCategoryNode);
        }

        if (!mispAttributeTagsList.isEmpty()) {
            ArrayNode mispAttributeTagsNode = jsonMapper.valueToTree(mispAttributeTagsList);
            mispNode.putArray("misp.attribute.tags").addAll(mispAttributeTagsNode);
        }

        return mispNode;
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
        FindEnrichableFields.setSrcEnrichmentFieldArray(srcEnrichmentFieldArray);
        FindEnrichableFields.setMispMappingMap(mispMappingMap);
    }
}
