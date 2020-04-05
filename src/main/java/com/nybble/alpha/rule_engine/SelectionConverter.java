package com.nybble.alpha.rule_engine;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

public class SelectionConverter {

    private static StringBuilder selectionFieldBuilder = new StringBuilder();
    private static StringBuilder transformFieldBuilder = new StringBuilder();
    private static StringBuilder transformBase64Builder = new StringBuilder();
    private static StringBuilder finalFieldBuilder = new StringBuilder();
    private static Boolean transformationAll = false;
    private static Boolean base64Flag = false;
    private static Boolean reFlag = false;
    private static String sigmaMapFile;
    private static String mapRuleId;
    private static String base64OffsetValues;
    private byte[] offset1 = new byte[1];
    private byte[] offset2 = new byte[2];
    private final static Map<String, ObjectNode> sigmaFieldsMappingMap = new HashMap<>();
    private static ObjectMapper jsonMapper = new ObjectMapper();

    // Map Sigma field to ECS field and convert non-array selection field as JsonPath
    String fieldConvert(String selectionKey, JsonNode selectionValue, String currentRuleId) throws InterruptedException, IOException {

        // Set Current Map RuleID
        mapRuleId = currentRuleId;
        // Start by setting sigmaFieldMappingMap
        setFieldMappingMap(currentRuleId);
        // Return string corresponding to JsonPath condition
        return selectionFieldBuilder(selectionKey, selectionValue);
    }

    // Map Sigma field to ECS field and convert array selection field as JsonPath
    String arrayFieldConvert(String arraySelectionKey, JsonNode arraySelectionValue, String currentRuleId) {

        // Set Current Map RuleID
        mapRuleId = currentRuleId;
        // Start by setting sigmaFieldMappingMap
        setFieldMappingMap(currentRuleId);

        // Reset String builder before using it
        finalFieldBuilder.setLength(0);
        finalFieldBuilder.append("(");
        AtomicInteger x = new AtomicInteger();

        // For each element (field) of Array return String corresponding to JsonPath condition.
        arraySelectionValue.forEach(element -> {

            // If field is also an Object, Iterate though Object in order to extract of K/V pair.
            // For each K/V pair return String corresponding to JsonPath.
            if (element.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> elementFields = element.fields();
                AtomicInteger y = new AtomicInteger();
                AtomicInteger z = new AtomicInteger();

                while (elementFields.hasNext()) {
                    Map.Entry<String, JsonNode> elementMap = elementFields.next();

                    if (elementMap.getValue().isArray()) {
                        for (int i = 0; i < elementMap.getValue().size(); i++) {

                            try {
                                finalFieldBuilder.append(selectionFieldBuilder(elementMap.getKey(), elementMap.getValue().get(i)));
                            } catch (InterruptedException | IOException e) {
                                e.printStackTrace();
                            }

                            if (z.get() + 1 < elementMap.getValue().size()) {
                                z.getAndIncrement();
                                // if Transformation 'all' is used, append logical AND between list values
                                if (transformationAll) {
                                    finalFieldBuilder.append(" && ");
                                    // else append logical OR list values.
                                } else {
                                    finalFieldBuilder.append(" || ");
                                }
                            }
                        }
                    } else {
                        try {
                            finalFieldBuilder.append(selectionFieldBuilder(elementMap.getKey(), elementMap.getValue()));
                        } catch (InterruptedException | IOException e) {
                            e.printStackTrace();
                        }
                    }

                    // Reset Value Modifier Transformation 'all' flag
                    transformationAll = false;

                    if (y.get() + 1 < element.size()) {
                        y.getAndIncrement();
                        // Because Object is a Map, append logical AND between each value of Map.
                        finalFieldBuilder.append(" && ");
                    }
                }
            } else {
                try {
                    finalFieldBuilder.append(selectionFieldBuilder(arraySelectionKey, element));
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                }
            }

            // If current element is not the last one
            if(x.get() < arraySelectionValue.size() - 1) {
                // if Transformation 'all' is used, append logical AND between list values
                if (transformationAll) {
                    finalFieldBuilder.append(" && ");
                    // else append logical OR list values.
                } else {
                    finalFieldBuilder.append(" || ");
                }
            }

            // If current element is the last one
            if(x.get() + 1 == arraySelectionValue.size()) {
                finalFieldBuilder.append(")");
            }

            // Increment counter x
            x.getAndIncrement();
        });

        // Reset Value Modifier Transformation 'all' flag
        transformationAll = false;
        return finalFieldBuilder.toString();
    }

    private String selectionFieldBuilder (String selectionKey, JsonNode selectionValue) throws InterruptedException, IOException {

        // Reset String builder before using it
        selectionFieldBuilder.setLength(0);
        String regexConvert;

        if (selectionValue.isTextual()) {
            String textValue = selectionValue.asText();

            // Append |contains because keyword is used to check if somethings appear in original message
            if (selectionKey.equals("keywords")) {
                selectionKey = selectionKey.concat("|contains");
            }

            // Check if Transformations (Value Modifiers) are used.
            // Then modify field value following used Transformation.
            if (selectionKey.contains("|")) {
                textValue = transformationFieldModifier(selectionKey, textValue);
                selectionKey = selectionKey.split("\\|")[0];
            }
            // Map fieldname from Sigma rule to fieldname from Map file.
            selectionKey = fieldMapping(selectionKey);

            //After mapping if default value is applied, some fields may need a transformation again.
            if (selectionKey.equals("message")) {
                selectionKey = selectionKey.concat("|contains");
                textValue = transformationFieldModifier(selectionKey, textValue);
                selectionKey = selectionKey.split("\\|")[0];
            }

            if (textValue.startsWith("*") && textValue.endsWith("*")) {
                regexConvert = regexCharacterEscape(textValue);
                regexConvert = regexConvert.replaceAll("\\*", ".*");
                selectionFieldBuilder.append("@.").append(selectionKey).append(" =~ ").append("/^").append(regexConvert).append("$/");
            } else if (textValue.startsWith("*")) {
                regexConvert = regexCharacterEscape(textValue);
                regexConvert = regexConvert.replaceAll("\\*", ".*");
                selectionFieldBuilder.append("@.").append(selectionKey).append(" =~ ").append("/^").append(regexConvert).append("/");
            } else if (textValue.endsWith("*")) {
                regexConvert = regexCharacterEscape(textValue);
                regexConvert = regexConvert.replaceAll("\\*", ".*");
                selectionFieldBuilder.append("@.").append(selectionKey).append(" =~ ").append("/").append(regexConvert).append("$/");
            } else if (textValue.contains("*")) {
                regexConvert = regexCharacterEscape(textValue);
                regexConvert = regexConvert.replaceAll("\\*", ".*");
                selectionFieldBuilder.append("@.").append(selectionKey).append(" =~ ").append("/").append(regexConvert).append("/");
            } else if (textValue.isEmpty()) {
                selectionFieldBuilder.append("@.").append(selectionKey).append(" == \"\"");
            } else {
                // Even when value is enclosed in single quote, backslash need to be escaped.
                selectionFieldBuilder.append("@.").append(selectionKey).append(" == ").append("'").append(textValue.replaceAll("\\\\", "\\\\\\\\")).append("'");
            }
        } else if (selectionValue.isNumber()) {
            // Check if Transformations (Value Modifiers) are used.
            // Then modify field value following used Transformation.
            if (selectionKey.contains("|")) {
                String textValue = transformationFieldModifier(selectionKey, selectionValue.asText());
                selectionKey = selectionKey.split("\\|")[0];

                // Map fieldname from Sigma rule to fieldname from Map file.
                selectionKey = fieldMapping(selectionKey);
                regexConvert = textValue.replaceAll("\\*", ".*");
                selectionFieldBuilder.append("@.").append(selectionKey).append(" =~ ").append("/").append(regexConvert).append("/");
            } else {
                // Map fieldname from Sigma rule to fieldname from Map file.
                selectionKey = fieldMapping(selectionKey);
                selectionFieldBuilder.append("@.").append(selectionKey).append(" == ").append(selectionValue.asLong());
            }
        } else if (selectionValue.isNull()) {
            // Map fieldname from Sigma rule to fieldname from Map file.
            selectionKey = fieldMapping(selectionKey);
            selectionFieldBuilder.append("@." + selectionKey + " == " + null);
        }

        // Reset base64Flag and reFlag
        base64Flag = false;
        reFlag = false;

        return selectionFieldBuilder.toString();
    }

    private String transformationFieldModifier (String selectionKey, String textValue) throws IOException {
        String[] transformationOperators = selectionKey.split("\\|");
        transformFieldBuilder.setLength(0);

        // If base64offset is in transformation operator, then use specific StringBuilder.
        List<String> operatorsList = Arrays.asList(transformationOperators);
        if (operatorsList.contains("base64offset")) {
            // Reset transfromBase64Builder
            transformBase64Builder.setLength(0);

            transformBase64Builder.append("(");

            //Create ByteArray to append byte offset to Base64 encoded textValue
            ByteArrayOutputStream appendOffset1 = new ByteArrayOutputStream();
            ByteArrayOutputStream appendOffset2 = new ByteArrayOutputStream();

            //Append 1 bytes to textValue
            appendOffset1.write(offset1);
            appendOffset1.write(textValue.getBytes());
            // Append 2 bytes to textValue
            appendOffset2.write(offset2);
            appendOffset2.write(textValue.getBytes());

            String b64TextValueOffset0 = Base64.getEncoder().withoutPadding().encodeToString(textValue.getBytes());
            String b64TextValueOffset1 = Base64.getEncoder().withoutPadding().encodeToString(appendOffset1.toByteArray());
            String b64TextValueOffset2 = Base64.getEncoder().withoutPadding().encodeToString(appendOffset2.toByteArray());

            b64TextValueOffset1 = b64TextValueOffset1.substring(0, b64TextValueOffset1.length() -1).substring(2);
            b64TextValueOffset2 = b64TextValueOffset2.substring(0, b64TextValueOffset2.length() -1).substring(4);

            transformBase64Builder.append(b64TextValueOffset0).append("|")
                    .append(b64TextValueOffset1).append("|")
                    .append(b64TextValueOffset2).append(")");

            base64OffsetValues = transformBase64Builder.toString();

            base64Flag = true;
        }

        for (int i = 1; i < transformationOperators.length; i++) {
            switch (transformationOperators[i]) {
                case "startswith":
                    if (base64Flag) {
                        transformFieldBuilder.append(base64OffsetValues).append("*");
                    } else {
                        transformFieldBuilder.append(textValue).append("*");
                    }
                    break;
                case "endswith":
                    if (base64Flag) {
                        transformFieldBuilder.append("*").append(base64OffsetValues);
                    } else {
                        transformFieldBuilder.append("*").append(textValue);
                    }
                    break;
                case "contains":
                    if (base64Flag) {
                        transformFieldBuilder.append("*").append(base64OffsetValues).append("*");
                    } else if (!textValue.startsWith("*") && !textValue.endsWith("*")) {
                        transformFieldBuilder.append("*").append(textValue).append("*");
                    } else {
                        transformFieldBuilder.append(textValue);
                    }
                    break;
                case "all":
                    transformationAll = true;
                    break;
                case "re":
                    reFlag = true;
                    break;
                case "base64":
                    transformFieldBuilder.append(Base64.getEncoder().encodeToString(textValue.getBytes()));
                    break;
                case "base64offset":
                    // Just break because base64 values has been prepared before.
                    break;
                default:
                    System.out.println("Value modifiers \"" + transformationOperators[i] + "\" operator is invalid or not yet supported");
                    break;
            }
        }
        return transformFieldBuilder.toString();
    }

    private String fieldMapping (String selectionKey) {

        if(sigmaFieldsMappingMap.containsKey(mapRuleId)) {
            try {
                ObjectNode currentRuleMapNode = sigmaFieldsMappingMap.get(mapRuleId);
                selectionKey = currentRuleMapNode.get("map").get("detection").get(selectionKey).asText();
            } catch (Exception e) {
                System.out.println("No corresponding field found in Mapping file for \"" + selectionKey + "\" for rule with ID : " + mapRuleId);
            }
        } else {
            System.out.println("No mapping file found for rule with ID : " + mapRuleId + ". Hold during mapping file creation.");
        }

        return selectionKey;
    }

    private String regexCharacterEscape (String textValue) {

        if (reFlag) {
            // No character escape
        } else if (base64Flag) {
            // No character escape
        } else {
            textValue = textValue.replaceAll("\\\\", "\\\\\\\\");
            textValue = textValue.replaceAll("/", Matcher.quoteReplacement("\\/"));
            textValue = textValue.replaceAll("\\.", "\\\\.");
            textValue = textValue.replaceAll("\\?", "\\\\?");
            textValue = textValue.replaceAll("\\+", "\\\\+");
            textValue = textValue.replaceAll("\\^", "\\\\^");
            textValue = textValue.replaceAll("\\$", "\\\\\\$");
            textValue = textValue.replaceAll("\\{", "\\\\{");
            textValue = textValue.replaceAll("}", "\\\\}");
            textValue = textValue.replaceAll("\\(", "\\\\(");
            textValue = textValue.replaceAll("\\)", "\\\\)");
            textValue = textValue.replaceAll("\\|", "\\\\|");
            textValue = textValue.replaceAll("\\[", "\\\\[");
            textValue = textValue.replaceAll("]", "\\\\]");
            textValue = textValue.replaceAll("'", "\\'");
        }

        return textValue;
    }

    public static void setFieldMappingMap(String currentRuleId) {

        try (InputStream inputMapFile = new FileInputStream(sigmaMapFile)) {

            ObjectNode sigmaMapNode = jsonMapper.readValue(inputMapFile, ObjectNode.class);
            // Add Map Node for current rule in sigmaFieldsMap.
            sigmaFieldsMappingMap.put(sigmaMapNode.get("id").toString(), sigmaMapNode);

        } catch (Throwable e) {
            System.out.println("ERROR: " + e.getMessage() + ". Sigma Map file for RuleID : " + currentRuleId + "is missing.");
        }
    }

    public ObjectNode getFieldsMappingMap(String ruleId) {

        ObjectNode fieldsMappingNode;
        fieldsMappingNode = sigmaFieldsMappingMap.get(ruleId);

        return fieldsMappingNode;
    }

    public static void setRulePath(String currentRulePath) {
        // Set SigmaMap Path to be able to set the SigmaMappingMap used for Mapping.
        String sigmaMapFolderPath = "./src/main/resources/SigmaMaps/";
        if (currentRulePath.endsWith(".yml")) {
            String mapFile = new File(currentRulePath).getName().replaceAll("\\.yml", ".json");
            sigmaMapFile = sigmaMapFolderPath + mapFile;
        } else if (currentRulePath.endsWith(".json")) {
            String mapFile = new File(currentRulePath).getName();
            sigmaMapFile = sigmaMapFolderPath + mapFile;
        }
    }
}
