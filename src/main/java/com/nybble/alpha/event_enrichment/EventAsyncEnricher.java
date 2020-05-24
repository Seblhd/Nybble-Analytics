package com.nybble.alpha.event_enrichment;

import com.nybble.alpha.NybbleAnalyticsConfiguration;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class EventAsyncEnricher extends RichAsyncFunction<ObjectNode, ObjectNode> {

    private transient RedisClient mispRedisClient;
    private transient ClientResources mispRedisClientResources;
    private transient RedisAsyncCommands<String, String> mispRedisAsyncCommands;
    private Long redisKeyExpiration = 86400L;
    private ObjectMapper jsonMapper = new ObjectMapper();
    private static Logger enrichmentEngineLogger = Logger.getLogger("enrichmentEngineFile");

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Create a configuration Object.
        NybbleAnalyticsConfiguration nybbleAnalyticsConfiguration = new NybbleAnalyticsConfiguration();

        // Set Redis Resources configuration.
        mispRedisClientResources = DefaultClientResources.builder()
                .computationThreadPoolSize(nybbleAnalyticsConfiguration.getRedisComputeThreads())
                .ioThreadPoolSize(nybbleAnalyticsConfiguration.getRedisIoThreads())
                .build();

        // Set Redis URI configuration.
        RedisURI mispRedisURI = RedisURI.Builder.redis(
                nybbleAnalyticsConfiguration.getRedisServerHost(),
                nybbleAnalyticsConfiguration.getRedisServerPort())
                .withDatabase(0)
                .build();

        mispRedisClient = RedisClient.create(mispRedisURI);
        mispRedisClient.setDefaultTimeout(Duration.ofMillis(nybbleAnalyticsConfiguration.getRedisConnectionTimeOut()));
        mispRedisAsyncCommands = mispRedisClient.connect().async();

        // Set Key expiration from config file
        redisKeyExpiration = nybbleAnalyticsConfiguration.getRedisKeyExpire();
    }

    @Override
    public void close() throws Exception {
        super.close();
        mispRedisClient.shutdown();
        mispRedisClientResources.shutdown();
    }

    @Override
    public void asyncInvoke(ObjectNode eventNode, ResultFuture<ObjectNode> eventNodeFuture) {

        ArrayList<Tuple5<String, String, String, String , ?>> enrichableFields = new EnrichableFieldsFinder().getList(eventNode);

        // mispRestSearchFields Tuple3 will contains all information for the RestSearch request.
        // f0 is : Event Tag
        // f1 is : Attribute Type
        // f2 is : Attribute Value
        // f3 is : Global type
        // f4 is : Enrichment option

        //System.out.println("Enrichable fields list : " + enrichableFields);

        if(!enrichableFields.isEmpty()) {
            enrichableFields.forEach(mispRequest -> {

                RedisFuture<String> mispRedisCache = mispRedisAsyncCommands.get(mispRequest.f2);

                try {
                    // Get MISP Attribute JSON String from Redis.
                    String mispRedisAttribute = mispRedisCache.get();
                    // If not null, enrich eventNode.
                    // If null, create REST query to MISP Server.
                    if (mispRedisAttribute != null) {
                        CompletableFuture.supplyAsync(() -> {
                            try {
                                // Create an ObjectNode with value from already existing key.
                                ObjectNode mispRedisAttributeNode = jsonMapper.readTree(mispRedisAttribute).deepCopy();
                                // If Node from Redis Cache is already containing information for current MISP Tag, then enrich.
                                // Else, create a REST Request to get information for a tag from MISP server and add them to Redis and enrich.
                                if (mispRedisAttributeNode.has(mispRequest.f0)) {
                                    eventNode.setAll(enrichEvent(mispRedisAttributeNode.get(mispRequest.f0).deepCopy()));
                                } else {
                                    ObjectNode mispRestRequestNode = new MispEnrichment().mispRequest(mispRequest.f0, mispRequest.f1, mispRequest.f2);
                                    // If REST Request has failed, an empty node is returned.
                                    if (!mispRestRequestNode.isEmpty()) {
                                        //ObjectNode redisCacheNode = jsonMapper.createObjectNode().set(mispRequest.f0, mispRestRequestNode);
                                        mispRedisAttributeNode.set(mispRequest.f0, mispRestRequestNode);
                                        //System.out.println("New ObjectNode for already existing is : " + mispRedisAttributeNode.toString());
                                        mispRedisAsyncCommands.set(mispRequest.f2, jsonMapper.writeValueAsString(mispRedisAttributeNode));
                                        // If Attribute Array is not empty enrich.
                                        if (!mispRestRequestNode.get("Attribute").isEmpty()) {
                                            eventNode.setAll(enrichEvent(mispRestRequestNode));
                                        }
                                    }
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            return eventNode;
                        }).thenAccept( (ObjectNode enrichedEventNode) -> eventNodeFuture.complete(Collections.singleton(enrichedEventNode)));
                    } else {
                        ObjectNode mispRestRequestNode = new MispEnrichment().mispRequest(mispRequest.f0, mispRequest.f1, mispRequest.f2);
                        if (!mispRestRequestNode.isEmpty()) {
                            ObjectNode redisCacheNode = jsonMapper.createObjectNode().set(mispRequest.f0, mispRestRequestNode);

                            // Write JSON String in Redis and set key expiration.
                            mispRedisAsyncCommands.set(mispRequest.f2, jsonMapper.writeValueAsString(redisCacheNode));
                            mispRedisAsyncCommands.expire(mispRequest.f2, redisKeyExpiration);
                            // If Attribute Array is not empty enrich.
                            if (!mispRestRequestNode.get("Attribute").isEmpty()) {
                                eventNode.setAll(enrichEvent(mispRestRequestNode));
                            }
                        }
                    }
                } catch (InterruptedException | ExecutionException | IOException e) {
                    e.printStackTrace();
                }
            });
        }

        eventNodeFuture.complete(Collections.singleton(eventNode));
    }

    private ObjectNode enrichEvent(ObjectNode mispResultNode) {

        // Create a Node that will contains information from MISP and then will be added to EventNode.
        ObjectNode mispNode = jsonMapper.createObjectNode();
        // Create an ArrayList that will contains all MISP Event ID where attribute value has been found.
        ArrayList<String> mispEventIDList = new ArrayList<>();
        // Create an ArrayList that will contains all unique MISP Category for the attribute value.
        ArrayList<String> mispAttributeCategoryList = new ArrayList<>();
        // Create an ArrayList that will contains all unique MISP Tags for the attribute value.
        ArrayList<String> mispAttributeTagsList = new ArrayList<>();

        for (int x = 0; x < mispResultNode.get("Attribute").size(); x++) {

            // Get each Event from MISP answer.
            ObjectNode mispEvent = mispResultNode.get("Attribute").get(x).deepCopy();

            // Add MISP Event ID from current Event.
            if (mispEvent.has("event_id")) {
                mispEventIDList.add(mispEvent.get("event_id").asText());
            }

            // Add MISP Attribute Category from current event.
            if (mispEvent.has("category")) {
                if(!mispAttributeCategoryList.contains(mispEvent.get("category").asText())) {
                    mispAttributeCategoryList.add(mispEvent.get("category").asText());
                }
            }

            // Add MISP Atrribute tags from current event.
            if (mispEvent.has("Tag")) {
                mispEvent.get("Tag").forEach(tag -> {
                    if (!mispAttributeTagsList.contains(tag.get("name").asText())) {
                        mispAttributeTagsList.add(tag.get("name").asText());
                    }
                });
            }
        }

        // If ArrayList is not empty, convert to ArrayNode and then add to final mispNode.
        if (!mispEventIDList.isEmpty()) {
            ArrayNode mispEventIDNode = jsonMapper.valueToTree(mispEventIDList);
            mispNode.putArray("misp.event.id").addAll(mispEventIDNode);
        }

        if (!mispAttributeCategoryList.isEmpty()) {
            ArrayNode mispAttributeCategoryNode = jsonMapper.valueToTree(mispAttributeCategoryList);
            mispNode.putArray("misp.attribute.category").addAll(mispAttributeCategoryNode);
        }

        if (!mispAttributeTagsList.isEmpty()) {
            ArrayNode mispAttributeTagsNode = jsonMapper.valueToTree(mispAttributeTagsList);
            mispNode.putArray("misp.attribute.tags").addAll(mispAttributeTagsNode);
        }

        return mispNode;
    }
}

