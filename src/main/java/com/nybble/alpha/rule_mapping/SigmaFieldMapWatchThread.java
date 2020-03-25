package com.nybble.alpha.rule_mapping;

import com.nybble.alpha.control_stream.SigmaRuleWatchThread;
import com.nybble.alpha.rule_engine.SelectionConverter;

import java.nio.file.*;

public class SigmaFieldMapWatchThread implements Runnable {

    private static String sigmaRulesFolder;
    private static String sigmaMapsFolder;
    private WatchService sigmaMapWatchService;

    public SigmaFieldMapWatchThread(WatchService sigmaWatchService, Path sigmaRulesPath, Path sigmaMapsPath) {
        sigmaMapWatchService = sigmaWatchService;
        sigmaRulesFolder = sigmaRulesPath.toString();
        sigmaMapsFolder = sigmaMapsPath.toString();
    }

    @Override
    public void run() {
        try {
            WatchKey sigmaMapWatchKey = sigmaMapWatchService.take();

            while (sigmaMapWatchKey != null) {
                for (WatchEvent sigmaRuleWatchEvent : sigmaMapWatchKey.pollEvents()) {
                    if (sigmaRuleWatchEvent.context().toString().endsWith(".json")) {
                        broadcastMappingModification(sigmaRuleWatchEvent.kind().toString(), sigmaRuleWatchEvent.context().toString());
                    }
                }
                sigmaMapWatchKey.reset();
                sigmaMapWatchKey = sigmaMapWatchService.take();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void broadcastMappingModification(String watchEventKind, String watchFile) throws Exception {

        switch (watchEventKind) {
            case "ENTRY_CREATE":
                System.out.println("Event Kind is : " + watchEventKind);
                // Create Strings with full path for Rules and Maps files.
                String sigmaCreatedMapFile = sigmaMapsFolder + "/" + watchFile;

                // Update the Mapping Map before creation and broadcast of rule with modification.
                // Do not create/update rule when Map file is added because it can be preparation and corresponding rule is probably not existing.
                SelectionConverter.setRulePath(watchFile);
                SelectionConverter.setFieldMappingMap(sigmaCreatedMapFile);
                break;
            case "ENTRY_MODIFY":
                System.out.println("Event Kind is : " + watchEventKind);
                // Create Strings with full path for Rules and Maps files.
                String sigmaRuleFile = sigmaRulesFolder + "/" + watchFile.replaceAll("\\.json", ".yml");
                String sigmaModifiedMapMFile = sigmaMapsFolder + "/" + watchFile;

                // Update the Mapping Map before creation and broadcast of rule with modification.
                SelectionConverter.setRulePath(watchFile);
                SelectionConverter.setFieldMappingMap(sigmaModifiedMapMFile);
                // Create new rules that will use new Mapping value before broadcast.
                SigmaRuleWatchThread.createSigmaRule(sigmaRuleFile);
                break;
            case "ENTRY_DELETE":
                break;
        }

    }
}
