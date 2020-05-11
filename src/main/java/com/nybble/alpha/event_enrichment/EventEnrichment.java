package com.nybble.alpha.event_enrichment;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.nybble.alpha.utils.JsonPathCheck;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

public class EventEnrichment implements MapFunction<ObjectNode, ObjectNode> {

    private ArrayList<Tuple3<String, String, String>> enrichableFields = new ArrayList<>();
    private static Boolean mispEnabledFlag = false;
    private static Logger enrichmentEngineLogger = Logger.getLogger("enrichmentEngineFile");
    private static ObjectMapper jsonMapper = new ObjectMapper();
    private static JsonPathCheck jsonPathCheck = new JsonPathCheck();

    @Override
    public ObjectNode map(ObjectNode eventNode) throws Exception {

        Boolean publicSrcIP = jsonPathCheck.getJsonBoolValue(eventNode, "$['nybble.source']['ip_public']");
        Boolean publicDestIP = jsonPathCheck.getJsonBoolValue(eventNode, "$['nybble.destination']['ip_public']");

        if (publicDestIP || publicSrcIP) {

            System.out.println("At least of of src or dst IP address is public.");

            if (eventNode.get("nybble.destination").get("ip_public").asBoolean()) {

                enrichableFields = new FindEnrichableFields().getList(eventNode);

                enrichableFields.forEach(mispRequest -> {
                    try {

                        ObjectNode mispAttributeNode = new MipsEnrichment().getAttributes(mispRequest.f0, mispRequest.f1, mispRequest.f2);

                        if (mispAttributeNode.get("Attribute").isEmpty()) {
                            enrichmentEngineLogger.info("MISP Response : attribute for value \"" + mispRequest.f2 +
                                    "\" with type \"" + mispRequest.f1 +
                                    "\" and tag \"" + mispRequest.f0 + "\" has not been found.");
                            // Put empty node in Redis to avoid search on MISP each time.
                        } else {
                            ObjectNode mispNode = new MipsEnrichment().enrichEvent(mispAttributeNode);
                            // Add information from MISP by merging mispNode and eventNode.
                            eventNode.setAll(mispNode);
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        }

        return eventNode;
    }

    public static void setMispEnabledFlag(Boolean enabledFlag) {
        mispEnabledFlag = enabledFlag;
    }
}
