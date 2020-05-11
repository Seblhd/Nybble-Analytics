package com.nybble.alpha.utils;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class JsonPathCheck {

    private final Configuration jsonPathConfigValue = Configuration.defaultConfiguration()
            .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
            .addOptions(Option.SUPPRESS_EXCEPTIONS);
    private final Configuration jsonPathConfigList = Configuration.defaultConfiguration()
            .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
            .addOptions(Option.SUPPRESS_EXCEPTIONS)
            .addOptions(Option.ALWAYS_RETURN_LIST);
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private static Logger nybbleAnalyticsLogger = Logger.getLogger("nybbleAnalyticsFile");

    public List<?> getJsonListValue(ObjectNode jsonNode, String jsonPath) throws JsonProcessingException {

        try {
            return JsonPath.using(jsonPathConfigList)
                    .parse(jsonMapper.writeValueAsString(jsonNode))
                    .read(jsonPath);
        } catch (JsonProcessingException jsonProcessEx) {
            nybbleAnalyticsLogger.error("JSON Patch check : " + jsonProcessEx);
            return new ArrayList<String>();
        }
    }

    public Boolean getJsonBoolValue(ObjectNode jsonNode, String jsonPath) {
        try {
            return JsonPath.using(jsonPathConfigValue)
                    .parse(jsonMapper.writeValueAsString(jsonNode))
                    .read(jsonPath);
        } catch (JsonProcessingException jsonProcessEx) {
            nybbleAnalyticsLogger.error("JSON Patch check : " + jsonProcessEx);
            return null;
        }
    }

    public String getJsonStringValue(ObjectNode jsonNode, String jsonPath) {
        try {
            return JsonPath.using(jsonPathConfigValue)
                    .parse(jsonMapper.writeValueAsString(jsonNode))
                    .read(jsonPath);
        } catch (JsonProcessingException jsonProcessEx) {
            nybbleAnalyticsLogger.error("JSON Patch check : " + jsonProcessEx);
            return null;
        }
    }

    public Long getJsonLongValue(ObjectNode jsonNode, String jsonPath) {
        try {
            return JsonPath.using(jsonPathConfigValue)
                    .parse(jsonMapper.writeValueAsString(jsonNode))
                    .read(jsonPath);
        } catch (JsonProcessingException jsonProcessEx) {
            nybbleAnalyticsLogger.error("JSON Patch check : " + jsonProcessEx);
            return null;
        }
    }

    public Integer getJsonIntValue(ObjectNode jsonNode, String jsonPath) {
        try {
            return JsonPath.using(jsonPathConfigValue)
                    .parse(jsonMapper.writeValueAsString(jsonNode))
                    .read(jsonPath);
        } catch (JsonProcessingException jsonProcessEx) {
            nybbleAnalyticsLogger.error("JSON Patch check : " + jsonProcessEx);
            return null;
        }
    }
}
