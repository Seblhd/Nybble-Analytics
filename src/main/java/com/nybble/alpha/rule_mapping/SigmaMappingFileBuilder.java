package com.nybble.alpha.rule_mapping;

import com.nybble.alpha.NybbleFlinkConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SigmaMappingFileBuilder {

    private static ObjectMapper jsonMapper = new ObjectMapper();
    private static ObjectMapper fileWriter = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static ObjectNode globalFieldMapping = jsonMapper.createObjectNode();
    private static ObjectNode sigmaMappingNode = fileWriter.createObjectNode();
    private Configuration nybbleFlinkConfiguration;
    private static Logger rulesMappingLogger = Logger.getLogger("ruleMappingFile");
    private String loggingRuleID;


    public SigmaMappingFileBuilder() throws IOException {

        // Set configuration values.
        NybbleFlinkConfiguration.setNybbleConfiguration();
        nybbleFlinkConfiguration = NybbleFlinkConfiguration.getNybbleConfiguration();

        // Create an ObjectNode containing Global Mapping fields.
        globalFieldMapping = jsonMapper.readValue(new File(nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.SIGMA_GLOBAL_MAP_PATH)), ObjectNode.class);
    }

    public void initFileBuilder(String sigmaRulePath, ObjectNode sigmaRule) throws IOException {

        // Get files in Maps Folder Path
        Path sigmaMapFolderPath = Paths.get(nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.SIGMA_MAPS_FOLDER_PATH));

        // Get RuleID for logging
        this.loggingRuleID = sigmaRule.get("id").toString();

        // Get the file name to create mapping file later.
        String sigmaFileName = Paths.get(sigmaRulePath).getFileName().toString().replaceAll("\\.yml", ".json");

        File sigmaJsonMapFile;
        if (!sigmaMapFolderPath.toString().endsWith("/")) {
            sigmaJsonMapFile = new File(sigmaMapFolderPath.toString()+"/"+sigmaFileName);
        } else {
            sigmaJsonMapFile = new File(sigmaMapFolderPath.toString()+sigmaFileName);
        }

        if (!sigmaJsonMapFile.exists()) {
            // If Map File doesn't exists create it.
            ObjectNode sigmaMappingFile = createMapFile(sigmaRule);
            // Write SigmaMappingFile content in JSON file.
            fileWriter.writeValue(sigmaJsonMapFile, sigmaMappingFile);

            rulesMappingLogger.info("Mapping file \"" + sigmaFileName + "\" has been created.");

        } else if (sigmaJsonMapFile.exists() && sigmaRule.has("action")) {
            if (sigmaRule.get("action").asText().equals("global")) {

                // If Map File exist but Rule is a MultiYaml Document, merge with existing file.
                ObjectNode sigmaMappingFile = createMapFile(sigmaRule);
                ObjectNode multiMappingNode = jsonMapper.readValue(sigmaJsonMapFile, ObjectNode.class);

                // Read GlobalDoc and merge already existing fields from RuleDoc
                ObjectReader objectReader = jsonMapper.readerForUpdating(multiMappingNode);
                objectReader.readValue(sigmaMappingFile);

                // Delete before new write.
                sigmaJsonMapFile.delete();

                // Write multiMappingFile content in JSON file.
                fileWriter.writeValue(sigmaJsonMapFile, multiMappingNode);

                rulesMappingLogger.info("Mapping file \"" + sigmaFileName + "\" has been updated.");
            }
        }
    }

    private ObjectNode createMapFile(ObjectNode sigmaRule) {

        // Reset SigmaMappingNode before new values.
        sigmaMappingNode.removeAll();

            // Iterate on "Detection field" to retrieve "Selection field(s)" and "Condition field"
            Iterator<Map.Entry<String, JsonNode>> sigmaDetectionField = sigmaRule.get("detection").fields();

            // Start by adding current ruleID in corresponding mapping File.
            sigmaMappingNode.put("id", sigmaRule.get("id").asText());

            for (int x = 0; x < sigmaRule.get("detection").size(); x++) {

                // Create an ObjectNode for selection fields.
                ObjectNode selectionFieldsNode = jsonMapper.createObjectNode();

                while (sigmaDetectionField.hasNext()) {

                    // Get value of next "Detection field"
                    Map.Entry<String, JsonNode> sigmaDetectionFieldMap = sigmaDetectionField.next();

                    // If "Detection field" key is not "condition" or "timeframe" then it's a "Selection field"
                    if(!sigmaDetectionFieldMap.getKey().equals("condition") && !sigmaDetectionFieldMap.getKey().equals("timeframe")) {

                        // Iterate on "Selection field(s)" to converter "selector" as JsonPath format.
                        Iterator<Map.Entry<String, JsonNode>> sigmaSelectionField = sigmaDetectionFieldMap.getValue().fields();

                        if(sigmaDetectionFieldMap.getValue().isArray()) {

                            sigmaDetectionFieldMap.getValue().forEach(element -> {
                                if (element.isObject()) {
                                    // Create an Iterator to retreive each fields from ObjectNode in Array
                                    Iterator<Map.Entry<String, JsonNode>> elementFields = element.fields();

                                    while (elementFields.hasNext()) {
                                        Map.Entry<String, JsonNode> elementMap = elementFields.next();
                                        // Then add each fields in rule with field name as Key and as Value.
                                        // The value will have to be change later to achieve the real field mapping.
                                        // This part is only for map file initialization and to avoid empty field if mapping is not done right after init.
                                        String selectionField = elementMap.getKey();

                                        if (selectionField.contains("|")) {
                                            selectionField = selectionField.split("\\|")[0];
                                        }

                                        if (!selectionFieldsNode.has(selectionField)) {
                                            selectionFieldsNode.put(selectionField, globalMapMapping(selectionField, globalFieldMapping));
                                        }
                                    }
                                } else {
                                    // Then add each fields in rule with field name as Key and as Value.
                                    // The value will have to be change later to achieve the real field mapping.
                                    // This part is only for map file initialization and to avoid empty field if mapping is not done right after init.
                                    String selectionField = sigmaDetectionFieldMap.getKey();

                                    if (selectionField.contains("|")) {
                                        selectionField = selectionField.split("\\|")[0];
                                    }

                                    if (!selectionFieldsNode.has(selectionField)) {
                                        selectionFieldsNode.put(selectionField, globalMapMapping(selectionField, globalFieldMapping));
                                    }
                                }
                            });
                        } else {
                            while (sigmaSelectionField.hasNext()) {

                                // Get value of next "Selection field"
                                Map.Entry<String, JsonNode> sigmaSelectionFieldMap = sigmaSelectionField.next();

                                // Then add each fields in rule with field name as Key and as Value.
                                // The value will have to be change later to achieve the real field mapping.
                                // This part is only for map file initialization and to avoid empty field if mapping is not done right after init.
                                String selectionField = sigmaSelectionFieldMap.getKey();

                                if (selectionField.contains("|")) {
                                    selectionField = selectionField.split("\\|")[0];
                                }

                                if (!selectionFieldsNode.has(selectionField)) {
                                    selectionFieldsNode.put(selectionField, globalMapMapping(selectionField, globalFieldMapping));
                                }
                            }
                        }
                        // Add selection field(s) from detection to mapping file.
                        sigmaMappingNode.putObject("map").set("detection", selectionFieldsNode);
                    } else if (sigmaDetectionFieldMap.getKey().equals("condition")) {

                        String condition = sigmaDetectionFieldMap.getValue().toString();

                        // Split search and aggregation expression
                        int conditionLength = condition.split(" \\| |\\|").length;

                        if (conditionLength == 2) {
                            // Add fields from aggregation in Mapping file.
                            String aggregationExpression = condition.split(" \\| |\\|")[1];

                            Matcher aggregationParser = Pattern.compile("(?<aggfunction>uniquecount|count|min|max|avg|sum)([(]?)(?<aggfield>\\w*?)([)]?)\\s?" +
                                    "(?:by)?\\s?(?<groupfield>\\w*?)\\s?(?<aggoperator><|>|=|<=|>=)\\s?(?<aggvalue>\\d*)").matcher(aggregationExpression);

                            while(aggregationParser.find()) {
                                // Map aggregation field if not empty and add to mapping node.
                                if (!aggregationParser.group("aggfield").isEmpty()) {
                                    selectionFieldsNode.put(aggregationParser.group("aggfield")
                                            , globalMapMapping(aggregationParser.group("aggfield"), globalFieldMapping));
                                }

                                // Map group field if not empty and add to mapping node.
                                if (!aggregationParser.group("groupfield").isEmpty()) {
                                    selectionFieldsNode.put(aggregationParser.group("groupfield")
                                            , globalMapMapping(aggregationParser.group("groupfield"), globalFieldMapping));
                                }
                            }
                        }
                    }
                }
            }

            if (sigmaRule.has("fields")) {
                if (sigmaRule.get("fields").isArray()) {
                    // Create an ObjectNode for field(s) fields.
                    ObjectNode fieldFieldsNode = jsonMapper.createObjectNode();

                    for (int y = 0; y <sigmaRule.get("fields").size(); y++) {
                        fieldFieldsNode.put(sigmaRule.get("fields").get(y).asText(), globalMapMapping(sigmaRule.get("fields").get(y).asText(), globalFieldMapping));
                    }
                    // Add field for fields to mapping file
                    sigmaMappingNode.with("map").set("fields", fieldFieldsNode);
                } else {
                    rulesMappingLogger.warn("Map creation : Invalid format for field \"fields\" in rule with ID \""+ sigmaRule.get("id") +"\", must be a List in Sigma Rule." +
                            " Useful fields will not be append to converted rule in Control Stream and fields will be missing in alerts.");
                    System.out.println("RuleID " + sigmaRule.get("id") + " : Invalid format for field \"fields\", must be a List in Sigma Rule");
                }
            }

        return sigmaMappingNode;
    }

    public void deleteMappingFile(String sigmaRuleFile) {

        Path sigmaMapFolderPath = Paths.get(nybbleFlinkConfiguration.getString(NybbleFlinkConfiguration.SIGMA_MAPS_FOLDER_PATH));

        File sigmaRuleMapPath = new File(sigmaMapFolderPath.toString()+"/"+sigmaRuleFile.replaceAll("\\.yml", ".json"));

        // Delete the JSON Mapping File responding to Sigma Rule.
        rulesMappingLogger.info("Map creation : Mapping file \"" + sigmaRuleMapPath.getName() + "\" has been deleted because corresponding rule has been deleted from Sigma Rules folder.");
        sigmaRuleMapPath.delete();
    }

    private String globalMapMapping(String ruleFieldMap, ObjectNode globalFieldMapping) {
        try {
            ruleFieldMap = globalFieldMapping.get("map").get(ruleFieldMap).asText();
        } catch (Exception e) {
            rulesMappingLogger.warn("Map creation : No corresponding field found in Mapping file for \"" + ruleFieldMap + "\" " +
                    "for rule with ID "+ loggingRuleID +" during Map file initialization." +
                    " Apply default with mapping field 'message', please modify mapping to a valid value to ensure rule trigger." );
            System.out.println("Map creation : No corresponding field found in Mapping file for \"" + ruleFieldMap + "\" during Map file initialization. " +
                    "Apply default with mapping field 'message'. ");
            ruleFieldMap = "message";
        }

        return ruleFieldMap;
    }
}
