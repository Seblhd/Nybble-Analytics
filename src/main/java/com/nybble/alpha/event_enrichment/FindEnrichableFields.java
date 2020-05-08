package com.nybble.alpha.event_enrichment;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FindEnrichableFields {

    // Create JsonPath configuration to search value of fields.
    private Configuration jsonPathConfig = Configuration.defaultConfiguration()
            .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
            .addOptions(Option.ALWAYS_RETURN_LIST)
            .addOptions(Option.SUPPRESS_EXCEPTIONS);
    private ObjectMapper jsonMapper = new ObjectMapper();
    private static ArrayList<String> srcEnrichmentFieldArray = new ArrayList<>();
    private static HashMap<String, ArrayList<Tuple2<String, String>>> mispMappingMap = new HashMap<>();
    private static ArrayList<Tuple3<String, String, String>> enrichableFieldList = new ArrayList<>();
    private static Logger enrichmentEngineLogger = Logger.getLogger("enrichmentEngineFile");

    public ArrayList<Tuple3<String, String, String>> getList (ObjectNode eventNode) {

        // Clear values in enrichableFieldList
        enrichableFieldList.clear();

        srcEnrichmentFieldArray.forEach(enrichmentField -> {

            // Search if field is in Event Node
            try {
                List<?> searchFieldValue = JsonPath.using(jsonPathConfig)
                        .parse(jsonMapper.writeValueAsString(eventNode))
                        .read("$." + enrichmentField);

                // If field has been found, then searchField value list contain the value.
                // Else, because of the "ALWAYS_RETURN_LIST" option, empty list is return when field has not bee found.
                if (!searchFieldValue.isEmpty()) {
                    searchFieldValue.forEach(foundValue -> {
                        if (foundValue != null) {

                            // When value for a specific field is found, get information from MISP Map to create Tuple3.
                            // mispRestSearchFields Tuple3 will contains all information for the RestSearch request.
                            // f0 is : Event Tag
                            // f1 is : Attribute Type
                            // f2 is : Attribute Value
                            try {
                                mispMappingMap.get(enrichmentField).forEach(MISPTuple2 -> {

                                    Tuple3<String, String, String> mispRestSearchFields = new Tuple3<>();
                                    mispRestSearchFields.setFields(MISPTuple2.f0, MISPTuple2.f1, foundValue.toString());

                                    if (!enrichableFieldList.contains(mispRestSearchFields)) {
                                        enrichableFieldList.add(mispRestSearchFields);
                                    }
                                });
                            } catch (NullPointerException nullField) {
                                enrichmentEngineLogger.error("Enrichable fields list creation : Field \"" + enrichmentField + "\" cannot be found in MISP Map whereas is in enrichable field list. Please review the JSON MISP Map for correction.");
                            }
                        }
                    });
                }
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

        });

        return enrichableFieldList;
    }

    public static void setSrcEnrichmentFieldArray(ArrayList<String> enrichFieldArray) {
        srcEnrichmentFieldArray = enrichFieldArray;
    }

    public static void setMispMappingMap(HashMap<String, ArrayList<Tuple2<String, String>>> mispMap) {
        mispMappingMap = mispMap;
    }
}
