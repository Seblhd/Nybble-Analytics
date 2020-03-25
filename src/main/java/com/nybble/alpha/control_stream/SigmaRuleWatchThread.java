package com.nybble.alpha.control_stream;

import com.nybble.alpha.event_stream.EventStreamTrigger;
import com.nybble.alpha.event_stream.MultipleEventProcess;
import com.nybble.alpha.rule_engine.MultiYamlConverter;
import com.nybble.alpha.rule_engine.RuleEngine;
import com.nybble.alpha.rule_engine.SelectionConverter;
import com.nybble.alpha.rule_mapping.SigmaMappingFileBuilder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SigmaRuleWatchThread implements Runnable {

    private static List<ObjectNode> sigmaLogSourceList = new ArrayList<>();
    private WatchService sigmaRuleWatchService;
    private static String sigmaRuleFolder;
    private static Integer ruleBroadcastCount = 0;
    private static Integer ruleFileCount = 0;
    private static Map<String, ObjectNode> sigmaRuleMap = new HashMap<>();
    private static Map<String, ObjectNode> sigmaRuleSaveStateMap = new HashMap<>();

    private static ObjectMapper jsonMapper = new ObjectMapper();
    private static Yaml yaml = new Yaml(new SafeConstructor());


    public SigmaRuleWatchThread(WatchService sigmaWatchService, Path sigmaFolder) {
        sigmaRuleWatchService = sigmaWatchService;
        sigmaRuleFolder = sigmaFolder.toString();
        // Initialize list containing all Sigma rules converted to JSON
        initSigmaRuleCollection(sigmaFolder);
    }

    @Override
    public void run() {
        try {
            WatchKey sigmaRuleWatchKey = sigmaRuleWatchService.take();

            while (sigmaRuleWatchKey != null) {
                for (WatchEvent sigmaRuleWatchEvent : sigmaRuleWatchKey.pollEvents()) {
                    if (sigmaRuleWatchEvent.context().toString().endsWith(".yml")) {
                        setSigmaRuleCollection(sigmaRuleWatchEvent.kind().toString(), sigmaRuleWatchEvent.context().toString());
                    }
                }
                sigmaRuleWatchKey.reset();
                sigmaRuleWatchKey = sigmaRuleWatchService.take();

            }
        } catch (Exception ex) {
            System.out.println("Exception:" + ex.toString());
        }
    }

    private void initSigmaRuleCollection(Path sigmaFolder) {

        // Start by clearing the Log Source list before initialization
        sigmaLogSourceList.clear();

        try (Stream<Path> sigmaRules = Files.walk(sigmaFolder)) {

            List<String> sigmaRulesList = sigmaRules.filter(path -> Files.isRegularFile(path) && path.toString().endsWith(".yml"))
                    .map(path -> path.toString()).collect(Collectors.toList());

            ruleFileCount = sigmaRulesList.size();
            EventStreamTrigger.setRuleFileCount(ruleFileCount);

            for (String path : sigmaRulesList) {
                // Set current rule path to be retrieve in Map File Creation.
                SelectionConverter.setRulePath(path);
                // For each Sigma Rule in YAML file, create JSONPath rule
                createSigmaRule(path);
            }
            EventStreamTrigger.setRuleBroadcastCount(ruleBroadcastCount);

        } catch (Exception e) {
            e.printStackTrace();
        }

        MultipleEventProcess.setSigmaLogSource(sigmaLogSourceList);
    }

    private void setSigmaRuleCollection(String watchEventKind, String watchFile) throws Exception {

        String ruleHash = org.apache.commons.codec.digest.DigestUtils.sha256Hex(sigmaRuleFolder + "/" + watchFile);
        String path = sigmaRuleFolder + "/" + watchFile;

        // Set current rule path to be retrieve in Map File Creation.
        SelectionConverter.setRulePath(path);

        if (watchEventKind.equals("ENTRY_CREATE")) {

            createSigmaRule(path);

            // Update Sigma Log Source list in case of new Log Source from new rule(s)
            updateSigmaLogSourceList();

        } else if (watchEventKind.equals("ENTRY_MODIFY")) {

            createSigmaRule(path);

            // Update Sigma Log Source list in case of new Log Source from updated rule(s)
            updateSigmaLogSourceList();

        } else if (watchEventKind.equals("ENTRY_DELETE")) {
            // Remove entry with Sigma Rule ID as key
            //sigmaRuleMap.remove(ruleHash);
            sigmaRuleSaveStateMap.remove(ruleHash);
            // Update Sigma Log Source list in case of old Log Source from deleted rule(s)
            updateSigmaLogSourceList();
            // Delete Mapping file corresponding to deleted rule
            new SigmaMappingFileBuilder().deleteMappingFile(watchFile);
        }
    }

    public static void createSigmaRule(String sigmaRulePath) throws Exception {

        ObjectNode sigmaConvertedNode;
        List<ObjectNode> yamlDocuments = new ArrayList<>();

        try (InputStream yamlFile = new FileInputStream(sigmaRulePath)) {
            for (Object yamlObject : yaml.loadAll(yamlFile)) {
                ObjectNode sigmaJsonObject = jsonMapper.readValue(jsonMapper.writeValueAsString(yamlObject), ObjectNode.class);
                yamlDocuments.add(sigmaJsonObject);
            }
        } catch (Throwable e) {
            System.out.println("ERROR: " + e.getMessage());
        }

        // If only 1 Yaml Document is in List then process as Single Documents YAML rule.
        if (yamlDocuments.size() == 1) {
            // Send Sigma converted rule to Mapping File Builder
            new SigmaMappingFileBuilder().initFileBuilder(sigmaRulePath ,yamlDocuments.get(0));

            sigmaConvertedNode = new RuleEngine().RuleInit(yamlDocuments.get(0));

            sigmaRuleMap.put(org.apache.commons.codec.digest.DigestUtils.sha256Hex(sigmaRulePath), sigmaConvertedNode);
            sigmaRuleSaveStateMap.put(org.apache.commons.codec.digest.DigestUtils.sha256Hex(sigmaRulePath), sigmaConvertedNode);
            // Add "category" and "product" from logsource field of rule in list
            createSigmaLogSourceList(yamlDocuments.get(0));
            // Increment the rule broadcast count after each collect. When broadcast count is > 1
            // Event stream is unpause for processing.
            ruleBroadcastCount += 1;

            // If there is more than 1 Yaml Document then process as Multi Documents YAML rule.
        } else if (yamlDocuments.size() > 1) {

            ObjectNode globalDoc = yamlDocuments.get(0);

            for (int yamlDocCount = 1; yamlDocCount < yamlDocuments.size(); yamlDocCount++) {

                ObjectNode mergedYamlDoc = new MultiYamlConverter().yamlDocMerger(globalDoc, yamlDocuments.get(yamlDocCount));

                // Send Sigma converted rule to Mapping File Builder
                new SigmaMappingFileBuilder().initFileBuilder(sigmaRulePath, mergedYamlDoc);

                sigmaConvertedNode = new RuleEngine().RuleInit(mergedYamlDoc);
                //System.out.println("Sigma converted node is : " + sigmaConvertedNode);
                sigmaRuleMap.put(org.apache.commons.codec.digest.DigestUtils.sha256Hex(sigmaRulePath), sigmaConvertedNode);
                sigmaRuleSaveStateMap.put(org.apache.commons.codec.digest.DigestUtils.sha256Hex(sigmaRulePath), sigmaConvertedNode);
                // Add "category" and "product" from logsource field of rule in list
                createSigmaLogSourceList(mergedYamlDoc);
                // Increment the rule broadcast count after each collect. When broadcast count is > 1
                // Event stream is unpause for processing.
                ruleBroadcastCount += 1;
            }
        }
    }

    Map<String, ObjectNode> getSigmaRuleMap() {
        return sigmaRuleMap;
    }

    void consumeSigmaRuleMap(String ruleHash) {
        sigmaRuleMap.remove(ruleHash);
    }

    private static void createSigmaLogSourceList(ObjectNode LogSourceNode) {

        ObjectNode filteredLogSource = jsonMapper.createObjectNode();

        if (LogSourceNode.get("logsource").has("category")
                && LogSourceNode.get("logsource").has("product")) {

            filteredLogSource.put("category", LogSourceNode.get("logsource").get("category").asText().toLowerCase());
            filteredLogSource.put("product", LogSourceNode.get("logsource").get("product").asText().toLowerCase());

        } else if (LogSourceNode.get("logsource").has("category")) {

            filteredLogSource.put("category", LogSourceNode.get("logsource").get("category").asText().toLowerCase());

        } else if (LogSourceNode.get("logsource").has("product")) {

            filteredLogSource.put("product", LogSourceNode.get("logsource").get("product").asText().toLowerCase());
        }

        if (!sigmaLogSourceList.contains(filteredLogSource)) {
            sigmaLogSourceList.add(filteredLogSource);
        }
    }

    public static void updateSigmaLogSourceList() {

        // Start by clearing the Log Source list before update
        sigmaLogSourceList.clear();

        try (Stream<Path> sigmaRules = Files.walk(Paths.get(sigmaRuleFolder))) {

            List<String> sigmaRulesList = sigmaRules.filter(path -> Files.isRegularFile(path) && path.toString().endsWith(".yml"))
                    .map(path -> path.toString()).collect(Collectors.toList());

            for (String path : sigmaRulesList) {

                List<ObjectNode> yamlDocuments = new ArrayList<>();

                try (InputStream yamlFile = new FileInputStream(path)) {
                    for (Object yamlObject : yaml.loadAll(yamlFile)) {
                        ObjectNode sigmaJsonObject = jsonMapper.readValue(jsonMapper.writeValueAsString(yamlObject), ObjectNode.class);
                        yamlDocuments.add(sigmaJsonObject);
                    }
                } catch (Throwable e) {
                    System.out.println("ERROR: " + e.getMessage());
                }

                // If only 1 Yaml Document is in List then process as Single Documents YAML rule.
                if (yamlDocuments.size() == 1) {
                    // Add "category" and "product" from logsource field of rule in list
                    createSigmaLogSourceList(yamlDocuments.get(0));

                    // If there is more than 1 Yaml Document then process as Multi Documents YAML rule.
                } else if (yamlDocuments.size() > 1) {

                    ObjectNode globalDoc = yamlDocuments.get(0);

                    for (int yamlDocCount = 1; yamlDocCount < yamlDocuments.size(); yamlDocCount++) {

                        ObjectNode mergedYamlDoc = new MultiYamlConverter().yamlDocMerger(globalDoc, yamlDocuments.get(yamlDocCount));
                        // Add "category" and "product" from logsource field of rule in list
                        createSigmaLogSourceList(mergedYamlDoc);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Update Sigma Source list.
        MultipleEventProcess.setSigmaLogSource(sigmaLogSourceList);
    }
}