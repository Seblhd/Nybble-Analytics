package com.nybble.alpha.rule_engine;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Iterator;

public class MultiYamlConverter {

    private static ObjectMapper jsonMapper = new ObjectMapper();
    private ObjectNode mergedGlobalDoc = jsonMapper.createObjectNode();
    private ObjectNode mergedRuleDoc = jsonMapper.createObjectNode();

    public ObjectNode yamlDocMerger (final ObjectNode globalDocument, final ObjectNode ruleDocuments) throws IOException {

        this.mergedRuleDoc = ruleDocuments.deepCopy();
        this.mergedGlobalDoc = globalDocument.deepCopy();

        Iterator<String> ruleFieldsIterator = ruleDocuments.fieldNames();

        while (ruleFieldsIterator.hasNext()) {
            String key = ruleFieldsIterator.next();

            if (mergedGlobalDoc.has(key)) {
                // Read GlobalDoc and merge already existing fields from RuleDoc
                ObjectReader objectReader = jsonMapper.readerForUpdating(mergedGlobalDoc);
                objectReader.readValue(mergedRuleDoc);

            } else if (!mergedGlobalDoc.has(key)) {
                // Add non-existing fields from RuleDoc to GlobalDOc
                mergedGlobalDoc.set(key, mergedRuleDoc.get(key));
            }
        }

        return mergedGlobalDoc;
    }
}
