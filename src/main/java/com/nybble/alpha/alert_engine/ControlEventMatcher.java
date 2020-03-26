package com.nybble.alpha.alert_engine;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonRawValue;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;
import com.jayway.jsonpath.*;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class ControlEventMatcher implements FlatMapFunction<Tuple2<ObjectNode, ArrayList<ObjectNode>>, ObjectNode> {

    private static String event;
    private ObjectMapper jsonMapper = new ObjectMapper();
    @JsonRawValue
    private String controlRule;

    @Override
    public void flatMap(Tuple2<ObjectNode, ArrayList<ObjectNode>> controlEventTuple, Collector<ObjectNode> collector) throws Exception {

        /*if (controlEventTuple.f0.get("winlog").get("event_id").asText().equals("4720")) {
            System.out.println("Value as String is : " + jsonMapper.writeValueAsString(controlEventTuple.f0.get("winlog").get("event_data")));
            event = controlEventTuple.f0.get("winlog").get("event_data").toString().replaceAll("\\\\", "\\\\\\\\");
            System.out.println("Event is :  " + event);
        }*/


        Configuration jsonPathConfig = Configuration.defaultConfiguration().addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL).addOptions(Option.SUPPRESS_EXCEPTIONS);


        for (ObjectNode controlNode : controlEventTuple.f1) {
            try {

                controlRule = controlNode.get("rule").get(0).get("jsonpathrule").asText();

                // Works with this config.
                List<ObjectNode> eventMatch = JsonPath.using(jsonPathConfig)
                        .parse(jsonMapper.writeValueAsString(controlEventTuple.f0))
                        .read(controlRule);

                //List<ObjectNode> eventMatch = JsonPath.read(jsonMapper.writeValueAsString(controlEventTuple.f0), controlRule);

                //List<ObjectNode> eventMatch = JsonPath.read(jsonMapper.writeValueAsString(controlEventTuple.f0),
                        //controlNode.get("rule").get(0).get("jsonpathrule").toString());

                //System.out.println("JsonPath Rule as String is : " + controlNode.get("rule").get(0).get("jsonpathrule").toString());
                //System.out.println("JsonPath Rule as Text is : " + controlNode.get("rule").get(0).get("jsonpathrule").asText());

                //$[?(@.winlog.event_data.TargetUserName =~ /^.*stien/ && @.log.level == "information" && @.winlog.event_data.UserAccountControl =~ /^.*\t\t%%2084/ && (@.winlog.event_id == 4720 || @.winlog.event_id == 4721))]

                //List<ObjectNode> eventMatch = JsonPath.read(controlEventTuple.f0.toString(),
                        //"$[?(@.winlog.event_data.TargetUserName =~ /^l.seb.*$/ && @.log.level == \"information\" && @.winlog.event_data.UserAccountControl =~ /.*\n\t\t%%2082\n\t\t%%2084/ && (@.winlog.event_id == 4720 || @.winlog.event_id == 4721))]");


                //List<ObjectNode> eventMatch = JsonPath.read(controlEventTuple.f0.toString(), "$[?(@.process.executable =~ /^.*\\GUP\\.exe/ && !(@.process.executable =~ /C:\\Users\\\\.*\\AppData\\Local\\Notepad\\+\\+\\updater\\gup\\.exe/ || @.process.executable =~ /C:\\Users\\\\.*\\AppData\\Roaming\\Notepad\\+\\+\\updater\\gup\\.exe/ || @.process.executable == 'C:\\Program Files\\Notepad++\\updater\\gup.exe' || @.process.executable == 'C:\\Program Files (x86)\\Notepad++\\updater\\gup.exe'))]");

                /*if (!eventMatch.isEmpty()) {
                    //System.out.println("Events are : " + controlEventTuple.f0.toString());
                    collector.collect(controlEventTuple.f0);
                }*/
            } catch (Exception e) {
                System.out.println("Error on rule : " + controlRule);
                e.printStackTrace();
            }

        }
    }
}
