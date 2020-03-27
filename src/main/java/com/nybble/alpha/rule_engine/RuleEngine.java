package com.nybble.alpha.rule_engine;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonRawValue;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.SerializableString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.io.CharacterEscapes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MapperFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.*;


public class RuleEngine {

    private static ObjectMapper jsonMapper = new ObjectMapper();
    private static final  ObjectMapper ruleNodeMapper = new ObjectMapper(new ObjectMapper().getFactory().setCharacterEscapes(new CharacterEscapes() {
        @Override
        public int[] getEscapeCodesForAscii() {
            int[] esc = CharacterEscapes.standardAsciiEscapesForJSON();
            esc['\\'] = CharacterEscapes.ESCAPE_NONE;
            return esc;
        }

        @Override
        public SerializableString getEscapeSequence(int i) {
            return null;
        }
    }));

    private static List fieldnameList = new ArrayList<>();
    private static List fieldValueList = new ArrayList<>();
    private static String mappedField;
    private static String jsonPathRule;

    public ObjectNode RuleInit(ObjectNode sigmaJsonRule) throws Exception {

        ObjectNode controlEvent = jsonMapper.createObjectNode();

        // Retrieve the current RuleID
        String currentRuleId = sigmaJsonRule.get("id").toString();

        // Iterate on "Detection field" to retrieve "Selection field(s)" and "Condition field"
        Iterator<Map.Entry<String, JsonNode>> sigmaDetectionField = sigmaJsonRule.get("detection").fields();

        // Instantiate Map for JsonPath converted "Selection field"
        Map<String, List<String>> convertedSelectionFieldMap = new HashMap<>();

        // Instantiate conditionFieldList condition  element from "conditionConverted" Class.
        List conditionFieldList = new ArrayList<>();

        ObjectNode conditionNode = jsonMapper.createObjectNode();

        for (int x = 0; x < sigmaJsonRule.get("detection").size(); x++) {
            while (sigmaDetectionField.hasNext()) {

                // Get value of next "Detection field"
                Map.Entry<String, JsonNode> sigmaDetectionFieldMap = sigmaDetectionField.next();

                // If "Detection field" key is not "condition" or "timeframe" then it's a "Selection field"
                if(!sigmaDetectionFieldMap.getKey().equals("condition") && !sigmaDetectionFieldMap.getKey().equals("timeframe")) {

                    // Iterate on "Selection field(s)" to converter "selector" as JsonPath format.
                    Iterator<Map.Entry<String, JsonNode>> sigmaSelectionField = sigmaDetectionFieldMap.getValue().fields();
                    // Instantiate List for JsonPath converted "Selection field"
                    List<String> convertedSelectionFieldList = new ArrayList<>();
                    List<String> convertedDetectionFieldList = new ArrayList<>();

                    if(sigmaDetectionFieldMap.getValue().isArray()) {

                        convertedDetectionFieldList.add(
                                new SelectionConverter().arrayFieldConvert(sigmaDetectionFieldMap.getKey(), sigmaDetectionFieldMap.getValue(), currentRuleId)
                        );
                        convertedSelectionFieldMap.put(sigmaDetectionFieldMap.getKey(), convertedDetectionFieldList);
                    }

                    while (sigmaSelectionField.hasNext()) {

                        // Get value of next "Selection field"
                        Map.Entry<String, JsonNode> sigmaSelectionFieldMap = sigmaSelectionField.next();

                        if (!fieldnameList.contains(sigmaSelectionFieldMap.getKey())) {
                            fieldnameList.add(sigmaSelectionFieldMap.getKey());
                        }

                        if (!fieldValueList.contains(sigmaSelectionFieldMap.getValue())) {
                            fieldValueList.add(sigmaSelectionFieldMap.getValue());
                        }

                        // If "Selection field" is not an Array convert field and put field in Map
                        if (sigmaSelectionFieldMap.getValue().isTextual() || sigmaSelectionFieldMap.getValue().isNumber() || sigmaSelectionFieldMap.getValue().isNull()) {
                            // Call Non-Array Selection Field converter
                            convertedSelectionFieldList.add(
                                    new SelectionConverter().fieldConvert(sigmaSelectionFieldMap.getKey(), sigmaSelectionFieldMap.getValue(), currentRuleId)
                            );

                            // If "Selection field" is an Array convert field with Logical OR between each field and put field in Map
                        } else if (sigmaSelectionFieldMap.getValue().isArray()) {
                            // Call Array Selection Field converter
                            convertedSelectionFieldList.add(
                                    new SelectionConverter().arrayFieldConvert(sigmaSelectionFieldMap.getKey(), sigmaSelectionFieldMap.getValue(), currentRuleId)
                            );
                        }

                        convertedSelectionFieldMap.put(sigmaDetectionFieldMap.getKey(), convertedSelectionFieldList);
                    }
                }

                if (sigmaDetectionFieldMap.getKey().equals("condition")) {

                    conditionNode = new ConditionConverter().conditionConvert(sigmaDetectionFieldMap.getValue());
                }
            }
        }

        // Create an Array Node to store each rule if there is multiple "Selection" in Sigma Rule.
        ArrayNode ruleArrayNode = jsonMapper.createArrayNode();

        for (int conArraySize = 0; conArraySize < conditionNode.get("condition").size(); conArraySize++) {

            // Create an ObjectNode to store element of each "Selection" in Sigma Rule
            ObjectNode ruleNode = ruleNodeMapper.createObjectNode();

            // Create an ObjectNode for each LogSource
            ObjectNode logSourceNode = jsonMapper.createObjectNode();

            for (JsonNode conditionFields : conditionNode.get("condition").get(conArraySize)) {
                conditionFieldList.add(conditionFields.asText());
            }

            // Call Rule builder to create final JsonPath string.
            jsonPathRule = new RuleBuilder().jsonPathBuilder(convertedSelectionFieldMap, conditionFieldList);

            // Append Sigma rule ID to Control Event.
            controlEvent.put("ruleid", sigmaJsonRule.get("id").asText());

            // Append Sigma Rule Title to Control Event
            controlEvent.put("ruletitle", sigmaJsonRule.get("title").asText());

            // Append final JsonPath rule to Rule Array Node
            ruleNode.put("jsonpathrule", jsonPathRule);

            // Append aggregation to Rule Array Node even if aggregation is null
            if (conditionNode.has("aggregation")) {
                ruleNode.set("aggregation", conditionNode.get("aggregation").get(conArraySize));
            } else if (!conditionNode.has("aggregation")) {
                String nullString = null;
                ruleNode.put("aggregation", nullString);
            }

            // Append timeframe to Rule Array Node even if timefram is null
            if (sigmaJsonRule.get("detection").has("timeframe")) {
                ObjectNode timeframNode = new ConditionConverter().timeframeConvert(sigmaJsonRule.get("detection").get("timeframe").asText());
                ruleNode.set("timeframe", timeframNode);
            } else if (!sigmaJsonRule.get("detection").has("timeframe")) {
                String nullString = null;
                ruleNode.put("timeframe", nullString);
            }

            if (sigmaJsonRule.get("logsource").has("category")
                    && sigmaJsonRule.get("logsource").has("product")) {
                // If event has both "Category" and "Product" fields
                // Then Key By "Category" and ""Product".
                logSourceNode.put("category", sigmaJsonRule.get("logsource").get("category").asText().toLowerCase());
                logSourceNode.put("product", sigmaJsonRule.get("logsource").get("product").asText().toLowerCase());
                ruleNode.set("logsource", logSourceNode);
            } else if (sigmaJsonRule.get("logsource").has("category")) {
                // If event only has "Product" field
                // Then Key By "Product".
                logSourceNode.put("category", sigmaJsonRule.get("logsource").get("category").asText().toLowerCase());
                ruleNode.set("logsource", logSourceNode);
            } else if (sigmaJsonRule.get("logsource").has("product")) {
                // If event only has "Category" field
                // Then Key By "Category".
                logSourceNode.put("product", sigmaJsonRule.get("logsource").get("product").asText().toLowerCase());
                ruleNode.set("logsource", logSourceNode);
            } else {
                // If event has none of "Category" or "Product" fields
                // Then return an empty ObjectNode.
                ruleNode.set("logsource", logSourceNode);
            }

            // Append a list containing tags to Control Event
            if (sigmaJsonRule.has("tags")) {
                ArrayNode tagsArray = getRuleTags(sigmaJsonRule.get("tags").deepCopy());
                ruleNode.set("tags", tagsArray);
            }

            // Append a list containing usefull fields to Control Event
            if (sigmaJsonRule.has("fields")) {
                ArrayNode fieldsArray = getUsefulFields(sigmaJsonRule.get("id").toString(), sigmaJsonRule.get("fields").deepCopy());
                ruleNode.set("fields", fieldsArray);
            }

            // Add each separate Rule Node in Rule Array Node
            ruleArrayNode.add(ruleNode);

            // Empty Condition Field List for next iteration
            conditionFieldList.clear();
        }

        // Append final ArrayNode containing all "Selection" from Sigma Rule to Event Control Node.
        controlEvent.putArray("rule").addAll(ruleArrayNode);

    return controlEvent;

    }

    private ArrayNode getRuleTags(ArrayNode tagsNode) {
        // Create an ArrayNode for tags
        ArrayNode tagsArray = jsonMapper.createArrayNode();
        if (tagsNode.isArray()) {
            // Create a temp list for unique value in Fields (In case of multiDoc)
            List<String> tagsList = new ArrayList<>();

            for (int y = 0; y < tagsNode.size(); y++) {
                if (!tagsList.contains(tagsNode.get(y).asText())) {
                    tagsList.add(tagsNode.get(y).asText());
                }
            }
            tagsList.forEach(tagsArray::add);
        } else {
            System.out.println("Invalid format for field \"tags\", must be a List in Sigma Rule");
        }

        return tagsArray;
    }

    private ArrayNode getUsefulFields(String currentRuleId, ArrayNode fieldsNode) {
        // Create an ArrayNode for mapped useful fields.
        ArrayNode fieldsArray = jsonMapper.createArrayNode();
        if (fieldsNode.isArray()) {

            //Retrieve the Field Mapping Map
            ObjectNode fieldMappingMapNode = new SelectionConverter().getFieldsMappingMap(currentRuleId);
            // Create a temp list for unique value in Fields (In case of multiDoc)
            List<String> mappedFieldList = new ArrayList<>();

            for (int y = 0; y <fieldsNode.size(); y++) {

                mappedField = fieldMappingMapNode.get("map").get("fields").get(fieldsNode.get(y).asText()).asText();

                if (!mappedFieldList.contains(mappedField)) {
                    mappedFieldList.add(mappedField);
                }
            }
            mappedFieldList.forEach(fieldsArray::add);

        } else {
            System.out.println("Invalid format for field \"fields\", must be a List in Sigma Rule");
        }

        return fieldsArray;
    }
}