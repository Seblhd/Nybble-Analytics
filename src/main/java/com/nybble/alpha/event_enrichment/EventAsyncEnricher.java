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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class EventAsyncEnricher extends RichAsyncFunction<ObjectNode, ObjectNode> {

    private transient RedisClient mispRedisClient;
    private transient RedisClient dnsRedisClient;
    private transient ClientResources redisClientResources;
    private transient RedisAsyncCommands<String, String> mispRedisAsyncCommands;
    private transient RedisAsyncCommands<String, String> dnsRedisAsyncCommands;
    private Long mispRedisKeyExpiration = 86400L;
    private Long dnsRedisKeyExpiration = 86400L;
    private ObjectMapper jsonMapper = new ObjectMapper();
    private static Logger enrichmentEngineLogger = Logger.getLogger("enrichmentEngineFile");

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Create a configuration Object.
        NybbleAnalyticsConfiguration nybbleAnalyticsConfiguration = new NybbleAnalyticsConfiguration();

        // Set Redis Resources configuration.
        redisClientResources = DefaultClientResources.builder()
                .computationThreadPoolSize(nybbleAnalyticsConfiguration.getRedisComputeThreads())
                .ioThreadPoolSize(nybbleAnalyticsConfiguration.getRedisIoThreads())
                .build();

        // Set MISP Redis URI configuration.
        RedisURI mispRedisURI = RedisURI.Builder.redis(
                nybbleAnalyticsConfiguration.getRedisServerHost(),
                nybbleAnalyticsConfiguration.getRedisServerPort())
                .withDatabase(nybbleAnalyticsConfiguration.getRedisMispCacheId())
                .build();

        // Set DNS Redis URI configuration.
        RedisURI dnsRedisURI = RedisURI.Builder.redis(
                nybbleAnalyticsConfiguration.getRedisServerHost(),
                nybbleAnalyticsConfiguration.getRedisServerPort())
                .withDatabase(nybbleAnalyticsConfiguration.getRedisDnsCacheId())
                .build();

        mispRedisClient = RedisClient.create(mispRedisURI);
        mispRedisClient.setDefaultTimeout(Duration.ofMillis(nybbleAnalyticsConfiguration.getRedisConnectionTimeOut()));
        mispRedisAsyncCommands = mispRedisClient.connect().async();

        dnsRedisClient = RedisClient.create(dnsRedisURI);
        dnsRedisClient.setDefaultTimeout(Duration.ofMillis(nybbleAnalyticsConfiguration.getRedisConnectionTimeOut()));
        dnsRedisAsyncCommands = dnsRedisClient.connect().async();

        // Set Key expiration from config file
        mispRedisKeyExpiration = nybbleAnalyticsConfiguration.getRedisMispKeyExpire();
        dnsRedisKeyExpiration = nybbleAnalyticsConfiguration.getRedisDnsKeyExpire();
    }

    @Override
    public void close() throws Exception {
        super.close();
        mispRedisClient.shutdown();
        dnsRedisClient.shutdown();
        redisClientResources.shutdown();
    }

    @Override
    public void asyncInvoke(ObjectNode eventNode, ResultFuture<ObjectNode> eventNodeFuture) {

        ArrayList<Tuple5<String, String, String, String , ?>> enrichableFields = new EnrichableFieldsFinder().getList(eventNode);

        // mispRestSearchFields Tuple5 will contains all information for the RestSearch request.
        // f0 is : Event Tag
        // f1 is : Attribute Type
        // f2 is : Attribute Value
        // f3 is : Global type
        // f4 is : Enrichment option

        //System.out.println("Enrichable fields list : " + enrichableFields);

        if(!enrichableFields.isEmpty()) {
            enrichableFields.forEach(mispRequest -> {

                // If Global type (mispRequest.f3) is "ip", then check "EnrichOnlyPublic" option value. This value determine if both Private and Public IP will be enriched or only Public IP.
                // Else If Global type (mispRequest.f3) is "domain", then check "ResolveName" option value. This value determine if Domain will be resolve and enrcih by MISP Request.
                // Else just try to enrich event without any option.
                if (mispRequest.f3.equals("ip")) {
                    // If Enrich Only Public IP is true, then check if IP is public.
                    // Else if Enrich Only Public IP is false, then enrich directly.
                    if ((Boolean) mispRequest.f4) {
                        try {
                            InetAddress requestedIp = InetAddress.getByName(mispRequest.f2);
                            if (!requestedIp.isSiteLocalAddress() && !requestedIp.isLoopbackAddress() && !requestedIp.isLinkLocalAddress() && !requestedIp.isMulticastAddress()) {

                                CompletableFuture.supplyAsync(() -> mispRedisRequest(eventNode, mispRequest)).thenAccept((ObjectNode enrichedEventNode) -> {
                                    eventNodeFuture.complete(Collections.singleton(enrichedEventNode));
                                });
                            }
                        } catch (UnknownHostException e) {
                            enrichmentEngineLogger.error("IP Enrichment : " + mispRequest.f2 + "is not a valid IP");
                        }
                    } else {
                        CompletableFuture.supplyAsync(() -> mispRedisRequest(eventNode, mispRequest)).thenAccept((ObjectNode enrichedEventNode) -> {
                            eventNodeFuture.complete(Collections.singleton(enrichedEventNode));
                        });
                    }
                } else if (mispRequest.f3.equals("domain")) {
                    // If ResolveName is true, then resolve domain and request MISP
                    // Else only request MISP.
                    if ((Boolean) mispRequest.f4) {

                        dnsCacheRedisRequest(mispRequest.f2);

                        CompletableFuture.supplyAsync(() -> mispRedisRequest(eventNode, mispRequest)).thenAccept((ObjectNode enrichedEventNode) -> {
                            eventNodeFuture.complete(Collections.singleton(enrichedEventNode));
                        });
                    } else {
                        CompletableFuture.supplyAsync(() -> mispRedisRequest(eventNode, mispRequest)).thenAccept((ObjectNode enrichedEventNode) -> {
                            eventNodeFuture.complete(Collections.singleton(enrichedEventNode));
                        });
                    }
                }
            });
        } else {
            eventNodeFuture.complete(Collections.singleton(eventNode));
        }
    }

    private ObjectNode mispRedisRequest(ObjectNode eventNode, Tuple5<String, String, String, String , ?> mispRequest) {

        RedisFuture<String> mispRedisCache = mispRedisAsyncCommands.get(mispRequest.f2);

        try {
            // Get MISP Attribute JSON String from Redis.
            String mispRedisAttribute = mispRedisCache.get();
            // If not null, enrich eventNode.
            // If null, create REST query to MISP Server.
            if (mispRedisAttribute != null) {
                try {
                    // Create an ObjectNode with value from already existing key.
                    ObjectNode mispRedisAttributeNode = jsonMapper.readTree(mispRedisAttribute).deepCopy();
                    //System.out.println("MISP Attributes are : " + mispRedisAttributeNode);
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
            } else {
                ObjectNode mispRestRequestNode = new MispEnrichment().mispRequest(mispRequest.f0, mispRequest.f1, mispRequest.f2);
                if (!mispRestRequestNode.isEmpty()) {
                    ObjectNode redisCacheNode = jsonMapper.createObjectNode().set(mispRequest.f0, mispRestRequestNode);

                    // Write JSON String in Redis and set key expiration.
                    mispRedisAsyncCommands.set(mispRequest.f2, jsonMapper.writeValueAsString(redisCacheNode));
                    mispRedisAsyncCommands.expire(mispRequest.f2, mispRedisKeyExpiration);
                    // If Attribute Array is not empty enrich.
                    if (!mispRestRequestNode.get("Attribute").isEmpty()) {
                        eventNode.setAll(enrichEvent(mispRestRequestNode));
                    }
                }
            }
        } catch (InterruptedException | ExecutionException | IOException e) {
            e.printStackTrace();
        }

        return eventNode;
    }

    private ObjectNode dnsCacheRedisRequest (String domainName) {

        // Create a Node that will contains information from DNS request and then will be added to EventNode.
        ObjectNode domainNode = jsonMapper.createObjectNode();

        RedisFuture<String> dnsRedisCache = dnsRedisAsyncCommands.get(domainName);

        try {
            // Get MISP Attribute JSON String from Redis.
            String dnsCacheValue = dnsRedisCache.get();

            // If not null, return IP for domain.
            // Else if null, create DNS request and store value in cache.
            if (dnsCacheValue != null) {
                try {
                    // Create an ObjectNode with value from already existing key.
                    ObjectNode dnsCacheRedisNode = jsonMapper.readTree(dnsCacheValue).deepCopy();

                    System.out.println("DNS Cache value is : " + dnsCacheRedisNode);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                // Resolve domain name.
                InetAddress domainAddress = InetAddress.getByName(domainName);
                // Create an ArrayList that will contains all resolved addresses.
                ArrayList<String> resolvedAddrArray = new ArrayList<>();
                resolvedAddrArray.add(domainAddress.getHostAddress());
                // Add array to Domain Node
                if (!resolvedAddrArray.isEmpty()) {
                    ArrayNode resolvedAddrArrayNode = jsonMapper.valueToTree(resolvedAddrArray);
                    domainNode.putArray("ipaddress").addAll(resolvedAddrArrayNode);

                    System.out.println("New resolved domain node for hostname \"" + domainName + "\" is  : " +  domainNode);

                    // Write JSON String in Redis and set key expiration.
                    dnsRedisAsyncCommands.set(domainName, jsonMapper.writeValueAsString(domainNode));
                    dnsRedisAsyncCommands.expire(domainName, dnsRedisKeyExpiration);
                }
            }

        } catch (InterruptedException | ExecutionException | UnknownHostException | JsonProcessingException e) {
            // In case of exception, return an empty node.
            return domainNode;
        }

        return domainNode;
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

