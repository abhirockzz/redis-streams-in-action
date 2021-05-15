package com.demo.azure;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.redisearch.Schema;
import io.redisearch.client.Client;
import io.redisearch.client.IndexDefinition;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;

public class StreamsConsumer {

    static JedisPool pool;
    static Client redisearch;

    static String INDEX_PREFIX = "tweet:";
    static String INDEX_NAME = "tweets-index";
    static String streamName;
    static String consumerGroupName;

    static int timeout = 5000;

    // Schema attributes
    static String SCHEMA_FIELD_ID = "id";
    static String SCHEMA_FIELD_USER = "user";
    static String SCHEMA_FIELD_TWEET = "text";
    static String SCHEMA_FIELD_LOCATION = "location";
    static String SCHEMA_FIELD_HASHTAGS = "hashtags";

    static Random random = new Random();

    static {

        streamName = System.getenv("STREAM_NAME");
        if (streamName == null) {
            throw new RuntimeException("Environment variable STREAM_NAME missing");
        }

        consumerGroupName = System.getenv("STREAM_CONSUMER_GROUP_NAME");
        if (consumerGroupName == null) {
            throw new RuntimeException("Environment variable STREAM_CONSUMER_GROUP_NAME missing");
        }

        String redisHost = System.getenv("REDIS_HOST");
        if (redisHost == null) {
            throw new RuntimeException("Environment variable REDIS_HOST missing");
        }

        String redisPort = System.getenv("REDIS_PORT");
        if (redisPort == null) {
            throw new RuntimeException("Environment variable REDIS_PORT missing");
        }

        String redisPassword = System.getenv("REDIS_PASSWORD");
        if (redisPassword == null) {
            throw new RuntimeException("Environment variable STREAM_CONSUMER_GROUP_NAME missing");
        }

        boolean isSSL = true;

        String ssl = System.getenv("SSL");
        if (ssl == null) {
            isSSL = false;
        }

        GenericObjectPoolConfig<Jedis> jedisPoolConfig = new GenericObjectPoolConfig<>();
        pool = new JedisPool(jedisPoolConfig, redisHost, Integer.valueOf(redisPort), timeout, redisPassword, isSSL);
        redisearch = new Client(INDEX_NAME, pool);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {

                if (redisearch != null) {
                    redisearch.close();
                }

                if (pool != null) {
                    pool.close();
                }

                System.out.println("Closed Redis pooled resources...");
            }
        }));

        Schema sc = new Schema().addTextField(SCHEMA_FIELD_ID, 1.0).addTextField(SCHEMA_FIELD_USER, 1.0)
                .addTextField(SCHEMA_FIELD_TWEET, 1.0).addTextField(SCHEMA_FIELD_LOCATION, 1.0)
                .addTagField(SCHEMA_FIELD_HASHTAGS);

        IndexDefinition def = new IndexDefinition().setPrefixes(new String[] { INDEX_PREFIX });

        try {
            boolean indexCreated = redisearch.createIndex(sc, Client.IndexOptions.defaultOptions().setDefinition(def));

            if (indexCreated) {
                System.out.println("Created RediSearch index ");
            }
        } catch (Exception e) {
            System.out.println("Did not create RediSearch index - " + e.getMessage());
        }
        Jedis conn = null;
        try {
            conn = pool.getResource();
            String res = conn.xgroupCreate(streamName, consumerGroupName, StreamEntryID.LAST_ENTRY, true);
            System.out.println("XGROUP CREATE result " + res);
        } catch (JedisDataException e) {
            if (!e.getMessage().contains("BUSYGROUP")) {
                throw e;
            }
            System.out.println("Group " + consumerGroupName + " already exists");
        } catch (Exception e) {
            throw e;
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    static Random r = new Random();

    public static void main(String[] args) throws Exception {

        String consumerName = "consumer-" + UUID.randomUUID().toString();
        System.out.println("consumer name " + consumerName);
        Jedis conn = null;

        try {
            conn = pool.getResource();

            while (true) {
                System.out.println("Reading from stream " + streamName + " with XREADGROUP");

                List<Entry<String, List<StreamEntry>>> results = conn.xreadGroup(consumerGroupName, consumerName, 500,
                        15000, false, Map.entry(streamName, StreamEntryID.UNRECEIVED_ENTRY));

                if (results == null) {
                    continue;
                }

                for (Entry<String, List<StreamEntry>> result : results) {
                    List<StreamEntry> entries = result.getValue();
                    for (StreamEntry entry : entries) {

                        String tweetid = entry.getFields().get("id");
                        String hashName = INDEX_PREFIX + tweetid;

                        try {
                            // simulate random failure/anomaly. ~ 20% will NOT be processed and ACKed. this
                            // will cause these entries to added to pending-entry-list (obtained via
                            // XPENDING)
                            if (!(random.nextInt(5) == 0)) {
                                conn.hset(hashName, entry.getFields());
                                System.out.println("saved tweet to hash " + hashName);

                                conn.xack(streamName, consumerGroupName, entry.getID());
                                // System.out.println("ACK tweet " + hashName);
                            } else {
                                System.out.println("not processed - " + hashName);
                            }
                        } catch (Exception e) {
                            System.out.println("FAILED to process" + hashName);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
}
