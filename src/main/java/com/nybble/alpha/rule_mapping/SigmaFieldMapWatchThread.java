package com.nybble.alpha.rule_mapping;

import com.nybble.alpha.control_stream.SigmaRuleWatchThread;
import com.nybble.alpha.rule_engine.SelectionConverter;
import org.apache.log4j.Logger;

import java.nio.file.*;

public class SigmaFieldMapWatchThread implements Runnable {

    private static String sigmaRulesFolder;
    private static String sigmaMapsFolder;
    private WatchService sigmaMapWatchService;
    private static Logger rulesMappingLogger = Logger.getLogger("ruleMappingFile");

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
                // Create Strings with full path for Rules and Maps files.
                String sigmaCreatedMapFile = sigmaMapsFolder + "/" + watchFile;

                // Update the Mapping Map before creation and broadcast of rule with modification.
                // Do not create/update rule when Map file is added because it can be preparation and corresponding rule is probably not existing.
                SelectionConverter.setRulePath(watchFile);
                SelectionConverter.setFieldMappingMap(sigmaCreatedMapFile);

                rulesMappingLogger.info("Mapping file \"" + watchFile + "\" has been added to Maps folder. Content has been added to Mapping map for future rule fields mapping.");

                break;
            case "ENTRY_MODIFY":
                // Create Strings with full path for Rules and Maps files.
                String sigmaRuleFile = sigmaRulesFolder + "/" + watchFile.replaceAll("\\.json", ".yml");
                String sigmaModifiedMapMFile = sigmaMapsFolder + "/" + watchFile;

                // Update the Mapping Map before creation and broadcast of rule with modification.
                SelectionConverter.setRulePath(watchFile);
                SelectionConverter.setFieldMappingMap(sigmaModifiedMapMFile);

                // Create new rules that will use new Mapping value before broadcast.
                SigmaRuleWatchThread.createSigmaRule(sigmaRuleFile);

                rulesMappingLogger.info("Mapping file \"" + watchFile + "\" has been modified in Maps folder. Content has been updated in Mapping map." +
                        " Sigma rule \"" + sigmaRuleFile + "\" has been updated with new field mapping and sent in Control Stream for update.");
                break;
            case "ENTRY_DELETE":

                rulesMappingLogger.info("Mapping file \"" + watchFile + "\" has been deleted from Sigma Maps folder. Corresponding rule has not been deleted and remain in Control Stream.");
                break;
        }

    }
}
