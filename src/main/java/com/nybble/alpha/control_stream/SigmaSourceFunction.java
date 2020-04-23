package com.nybble.alpha.control_stream;

import com.nybble.alpha.NybbleAnalyticsConfiguration;
import com.nybble.alpha.rule_mapping.SigmaFieldMapWatchThread;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.log4j.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchService;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SigmaSourceFunction implements SourceFunction<ObjectNode> {

    private volatile boolean isRunning = true;
    private SigmaRuleWatchThread sigmaRulesWatchThread;
    private Map<String, ObjectNode> sigmaStreamMap = new ConcurrentHashMap<>();
    private static Logger controlStreamLogger = Logger.getLogger("controlStreamFile");

    @Override
    public void run(SourceContext<ObjectNode> sCtx) {

        // Create new Nybble Analytics configuration object to get path.
        NybbleAnalyticsConfiguration nybbleAnalyticsConfiguration = new NybbleAnalyticsConfiguration();

        // Set path for Rules folder.
        Path sigmaRulesFolderPath = Paths.get(nybbleAnalyticsConfiguration.getSigmaRulesFolder());

        // Set path for Maps folder.
        Path sigmaMapsFolderPath = Paths.get(nybbleAnalyticsConfiguration.getSigmaMapsFolder());

        try {
            // Create a Watch Service and Thread to monitor Sigma Rules Folder.
            WatchService sigmaRulesWatchService = sigmaRulesFolderPath.getFileSystem().newWatchService();
            sigmaRulesWatchThread = new SigmaRuleWatchThread(sigmaRulesWatchService, sigmaRulesFolderPath);
            Thread rulesThread = new Thread(sigmaRulesWatchThread, "Sigma Rules Watch service thread");
            rulesThread.start();
            sigmaRulesFolderPath.register(sigmaRulesWatchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE,
                    StandardWatchEventKinds.ENTRY_MODIFY);
            // Create a Watch Service and Thread to monitor Sigma Maps Folder.
            WatchService sigmaMapsWatchService = sigmaMapsFolderPath.getFileSystem().newWatchService();
            SigmaFieldMapWatchThread sigmaMapsWatchThread = new SigmaFieldMapWatchThread(sigmaMapsWatchService, sigmaRulesFolderPath, sigmaMapsFolderPath);
            Thread mapsThread = new Thread(sigmaMapsWatchThread, "Sigma Maps Watch service thread");
            mapsThread.start();
            sigmaMapsFolderPath.register(sigmaMapsWatchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE,
                    StandardWatchEventKinds.ENTRY_MODIFY);
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }

        while (isRunning) {
            // Get the SigmaRule Map
            sigmaStreamMap = sigmaRulesWatchThread.getSigmaRuleMap();

            // If SigmaRule Map is not empty, get ObjectNode and send to Stream.
            if(!sigmaStreamMap.isEmpty()) {
                    // Use Iterator to avoid ConcurrentModificationException
                for (Iterator<String> ruleHashIterator = sigmaStreamMap
                        .keySet().iterator(); ruleHashIterator.hasNext();) {
                    // Store next() Rule Hash value
                    String ruleHash = ruleHashIterator.next();
                    // Collect Sigma JSON from Map and send to Stream
                    sCtx.collect(sigmaStreamMap.get(ruleHash));

                    controlStreamLogger.info("Broadcasted rule : " + sigmaStreamMap.get(ruleHash).toString());

                    // Remove Rule Hash from sigmaStreamMap once streamed
                    ruleHashIterator.remove();
                    // Remove Sigma JSON from Map once this one have been
                    sigmaRulesWatchThread.consumeSigmaRuleMap(ruleHash);
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}