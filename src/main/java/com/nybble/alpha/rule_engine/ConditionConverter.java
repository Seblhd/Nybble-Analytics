package com.nybble.alpha.rule_engine;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConditionConverter {

    private static ObjectMapper jsonMapper = new ObjectMapper();
    private ObjectNode aggMappingNode;
    private static Logger ruleEngineLogger = Logger.getLogger("ruleEngineFile");
    private String currentRuleID;

    public ConditionConverter(String currrentRuleID) throws IOException {
        // Get MappingNode for current rule to map fields in aggregation if exists.
        aggMappingNode = new SelectionConverter().getFieldsMappingMap(currrentRuleID);
        // Store current rule ID for logging.
        this.currentRuleID = currrentRuleID;
    }

    public ObjectNode conditionConvert(JsonNode conditions) {

        ObjectNode conditionNode = jsonMapper.createObjectNode();

        if (conditions.getNodeType().toString().equals("ARRAY")) {

            ObjectNode conditionBuildNode;
            ArrayNode conditionNestedList;
            ArrayNode aggregationArrayNode = jsonMapper.createArrayNode();
            ArrayNode conditionArrayNode = jsonMapper.createArrayNode();

            for (JsonNode conditionList : conditions) {
                // For each condition in list call 'conditionBuilder' to extract rules information.
                conditionBuildNode = conditionBuilder(conditionList.toString());
                // Get value (Array) for 'condition' key and insert this one in ArrayNode.
                conditionNestedList = conditionBuildNode.get("condition").get(0).deepCopy();
                conditionArrayNode.insertArray(0).addAll(conditionNestedList);

                // Get each Aggregation Node and insert this one in ArrayNode
                aggregationArrayNode.add(conditionBuildNode.get("aggregation").get(0));
            }

            conditionNode.putArray("condition").addAll(conditionArrayNode);
            conditionNode.putArray("aggregation").addAll(aggregationArrayNode);


        } else if (conditions.getNodeType().toString().equals("STRING")) {
            conditionNode = conditionBuilder(conditions.toString());
        }

        return conditionNode;
    }

    public ObjectNode conditionBuilder(String condition) {

        List<String> searchElement = new ArrayList<>();

        ArrayNode nestedArrayNode = jsonMapper.createArrayNode();
        ArrayNode aggregationArray = jsonMapper.createArrayNode();
        ObjectNode conditionBuilderNode = jsonMapper.createObjectNode();

        // Split search and aggregation expression
        int conditionLength = condition.split(" \\| |\\|").length;
        String searchExpression = condition.split(" \\| |\\|")[0];

        // Aggregation Expression to support with Dynamic stream creation
        if (conditionLength == 2) {
            String aggregationExpression = condition.split(" \\| |\\|")[1];

            // Add ObjectNode returned by aggregationConvert function to Aggregation Array Node.
            aggregationArray.add(aggregationConvert(aggregationExpression));
            conditionBuilderNode.putArray("aggregation").addAll(aggregationArray);
        }

        // Match search expression with regex to extract operator and "Selection" fields
        Matcher searchParser = Pattern.compile("([(]?|[)]?)\\s?(all of them|1 of them|1 of|all of|\\w+[*]?)\\s?([)]?|[(]?)\\s?" +
                "(and|or|not|1 of|all of|)?([(]?|[)]?)\\s?(not|1 of|all of|)?([)]?|[(]?)\\s?").matcher(searchExpression);

        while(searchParser.find()){
            for(int z = 1; z < searchParser.groupCount() + 1; z++) {
                if (!searchParser.group(z).isEmpty()) {
                    searchElement.add(searchParser.group(z));
                }
            }
        }

        // Create an ArrayNode containing Search Elements and then add ArrayNode to NestedArrayNode
        ArrayNode conditionArrayNode = jsonMapper.valueToTree(searchElement);
        nestedArrayNode.insertArray(0).addAll(conditionArrayNode);

        // Insert Nested ArrayNode in return conditionNode.
        conditionBuilderNode.putArray("condition").addAll(nestedArrayNode);
        return conditionBuilderNode;
    }

    private ObjectNode aggregationConvert (String aggregationExpression) {

        ObjectNode aggregationNode = jsonMapper.createObjectNode();

        Matcher aggregationParser = Pattern.compile("(?<aggfunction>uniquecount|count|min|max|avg|sum)([(]?)(?<aggfield>\\w*?)([)]?)\\s?" +
                "(?:by)?\\s?(?<groupfield>\\w*?)\\s?(?<aggoperator><|>|=|<=|>=)\\s?(?<aggvalue>\\d*)").matcher(aggregationExpression);

        while(aggregationParser.find()) {

            aggregationNode.put("aggfunction", aggregationParser.group("aggfunction"));

            // Map aggregation field if not empty
            if (!aggregationParser.group("aggfield").isEmpty()) {
                String aggFieldMapping = fieldMapping(aggregationParser.group("aggfield"));
                aggregationNode.put("aggfield", aggFieldMapping);
            } else {
                aggregationNode.put("aggfield", aggregationParser.group("aggfield"));
            }

            // Map group field if not empty
            if (!aggregationParser.group("groupfield").isEmpty()) {
                String groupFieldMapping = fieldMapping(aggregationParser.group("groupfield"));
                aggregationNode.put("groupfield", groupFieldMapping);
            } else {
                aggregationNode.put("groupfield", aggregationParser.group("groupfield"));
            }

            aggregationNode.put("aggoperator", aggregationParser.group("aggoperator"));
            aggregationNode.put("aggvalue", aggregationParser.group("aggvalue"));
        }

        return aggregationNode;
    }

    public ObjectNode timeframeConvert (String timeframe) {

        ObjectNode timeframeNode = jsonMapper.createObjectNode();

        Matcher timeframeParser = Pattern.compile("(?<duration>\\d*)(?<unit>s|m|h|d|M|y)").matcher(timeframe);

        while(timeframeParser.find()) {
            if (timeframeParser.group("unit").equals("s")) {
                timeframeNode.put("duration", Long.parseLong(timeframeParser.group("duration")));
                timeframeNode.put("unit", timeframeParser.group("unit"));
            } else {
                long duration = convertToSecond(Long.parseLong(timeframeParser.group("duration")), timeframeParser.group("unit"));
                timeframeNode.put("duration", duration);
                timeframeNode.put("unit", "s");
            }

        }

        return timeframeNode;
    }

    private String fieldMapping (String selectionKey) {
        try {
            selectionKey = aggMappingNode.get("map").get("detection").get(selectionKey).asText();
        } catch (Exception e) {
            ruleEngineLogger.warn("Aggregation field mapping : No corresponding field found in Mapping file during condition conversion for field \"" + selectionKey + "\" for rule with ID : " + currentRuleID);
            System.out.println("No corresponding field found in Mapping file for \"" + selectionKey + "\"");
        }

        return selectionKey;
    }

    private Long convertToSecond (Long duration, String unit) {

        long seconds = 0L;

        switch (unit) {
            case "m":
                seconds = TimeUnit.MINUTES.toSeconds(duration);
                break;
            case "h":
                seconds = TimeUnit.HOURS.toSeconds(duration);
                break;
            case "d":
                seconds = TimeUnit.DAYS.toSeconds(duration);
                break;
            case "M":
                // Average
                seconds = TimeUnit.DAYS.toSeconds(duration) * 30;
                break;
            case "y":
                // Standard year
                seconds = TimeUnit.DAYS.toSeconds(duration) * 365;
                break;
        }
        return seconds;
    }
}

