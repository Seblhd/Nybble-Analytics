package com.nybble.alpha.event_enrichment;

import com.nybble.alpha.utils.JsonPathCheck;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class EnrichableFieldsFinder {

    private static ArrayList<String> srcEnrichmentFieldArray = new ArrayList<>();
    private static HashMap<String, ArrayList<Tuple>> mispMappingMap = new HashMap<>();
    private static Logger enrichmentEngineLogger = Logger.getLogger("enrichmentEngineFile");
    private static JsonPathCheck jsonPathCheck = new JsonPathCheck();

    public ArrayList<Tuple5<String, String, String, String, ?>> getList (ObjectNode eventNode) {

        // Create new enrichableFieldList
        ArrayList<Tuple5<String, String, String, String, ?>> enrichableFieldList = new ArrayList<>();

        srcEnrichmentFieldArray.forEach(enrichmentField -> {

            // Search if field is in Event Node
            try {

                String jsonPath = "$." + enrichmentField;
                List<?> searchFieldValue = jsonPathCheck.getJsonListValue(eventNode, jsonPath);

                // If field has been found, then searchField value list contain the value.
                // Else, because of the "ALWAYS_RETURN_LIST" option, empty list is return when field has not bee found.
                if (!searchFieldValue.isEmpty()) {
                    searchFieldValue.forEach(foundValue -> {
                        if (foundValue != null) {

                            // When value for a specific field is found, get information from MISP Map to create Tuple3.
                            // mispRestSearchFields Tuple5 will contains all information for the RestSearch request.
                            // f0 -> MISP Event tag
                            // f1 -> MISP Attribute Type.
                            // f2 -> MISP Attribute Value
                            // f3 -> Global type
                            // f4 -> Enrichment option
                            try {
                                mispMappingMap.get(enrichmentField).forEach(MispTuple -> {

                                    if (MispTuple.getField(2).toString().equals("ip") || MispTuple.getField(2).toString().equals("domain")) {
                                        // Create Tuple5 and add information for request
                                        Tuple5<String, String, String, String, Boolean> mispRestSearchFields = new Tuple5<>();
                                        mispRestSearchFields.setFields(MispTuple.getField(0), // Is Tags from MISP Mapping Map
                                                MispTuple.getField(1), // Is Attribute Type from MISP Mapping Map
                                                foundValue.toString(), // Is Attribute Value from JsonPathCheck
                                                MispTuple.getField(2), // Is Global type from MISP Mapping Map
                                                MispTuple.getField(3)); // Is Enrichment option from MISP Mapping Map. Set to null for Global type without options.
                                        if (!enrichableFieldList.contains(mispRestSearchFields)) {
                                            enrichableFieldList.add(mispRestSearchFields);
                                        }
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

    public static void setMispMappingMap(HashMap<String, ArrayList<Tuple>> mispMap) {
        mispMappingMap = mispMap;
    }
}
