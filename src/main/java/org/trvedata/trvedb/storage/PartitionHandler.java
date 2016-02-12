package org.trvedata.trvedb.storage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trvedata.trvedb.ChannelKey;
import org.trvedata.trvedb.websocket.ClientConnection;
import io.dropwizard.lifecycle.Managed;

/**
 * A thread that consumes a single partition from Kafka, and maintains all the
 * channels contained in that partition. Public methods may be called from
 * anywhere and must therefore be thread-safe.
 */
public class PartitionHandler implements Runnable, Managed {

    private static final Logger log = LoggerFactory.getLogger(PartitionHandler.class);
    private final TopicPartition topicPartition;
    private final Properties consumerConfig;
    private final Producer<ChannelKey, byte[]> producer;
    private final ConcurrentMap<String, Channel> channels = new ConcurrentHashMap<>();
    private final ConcurrentMap<ClientConnection, Set<String>> clientSubscriptions = new ConcurrentHashMap<>();
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private final AtomicBoolean threadFailed = new AtomicBoolean(false);
    private final CompletableFuture<Void> startupFuture = new CompletableFuture<>();
    private final Path storagePath;
    private final Thread thread;
    private Consumer<ChannelKey, byte[]> consumer = null;
    private ColumnFamily<ChannelKey, byte[]> messageStore = null;

    public PartitionHandler(TopicPartition topicPartition, Properties consumerConfig,
                            Producer<ChannelKey, byte[]> producer) {
        this.topicPartition = topicPartition;
        this.consumerConfig = consumerConfig;
        this.producer = producer;
        String name = topicPartition.topic() + "-" + topicPartition.partition();
        this.storagePath = StreamStore.STORAGE_PATH.resolve(name);
        this.thread = new Thread(this, "PartitionHandler [" + name + "]");
    }

    public static Properties consumerConfig() {
        Properties config = new Properties();
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ChannelKey.KeyDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return config;
    }

    public static Properties producerConfig() {
        Properties config = new Properties();
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ChannelKey.KeySerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // The Kafka producer maintains a fixed-size buffer for messages being sent to
        // the brokers. If this buffer fills up, send requests will block for up to
        // MAX_BLOCK_MS_CONFIG milliseconds waiting for buffer space to free up. If this
        // waiting time is exceeded, sending requests throw TimeoutException. We set the
        // maximum blocking time fairly short, to allow us to shed load and free up
        // threads if the producer is overloaded.
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024L);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        return config;
    }

    @Override
    public void start() throws Exception {
        thread.start();
        try {
            // Waits until the thread we just started indicates that it has
            // started up successfully. If an exception is thrown during startup,
            // it is re-thrown here.
            startupFuture.get(60, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new IOException("Failed to start up PartitionHandler in time", e);
        }
    }

    @Override
    public void stop() throws Exception {
        log.info("Shutdown requested for PartitionHandler {}", topicPartition);
        shutdownRequested.set(true);
        if (consumer != null) consumer.wakeup();
    }

    public void waitForShutdown() {
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    @Override
    public void run() {
        // Ought to call #configure on serializers, but they're no-ops in this case
        Serdes<ChannelKey, byte[]> messageStoreSerdes = new Serdes<>(new ChannelKey.KeySerializer(),
                new ChannelKey.KeyDeserializer(),
                new ByteArraySerializer(),
                new ByteArrayDeserializer());

        try (
            Consumer<ChannelKey, byte[]> consumer = new KafkaConsumer<>(consumerConfig);
            KeyValueStore store = new KeyValueStore(storagePath);
        ) {
            this.consumer = consumer;
            messageStore = store.addColumnFamily("messages", messageStoreSerdes);
            store.open();
            consumer.assign(Arrays.asList(topicPartition));
            consumer.seekToBeginning(topicPartition); // TODO resume from last offset

            // If we got to this point, consider the thread successfully started up
            startupFuture.complete(null);

            while (!shutdownRequested.get()) {
                ConsumerRecords<ChannelKey, byte[]> records = consumer.poll(1000);
                for (ConsumerRecord<ChannelKey, byte[]> record : records.records(topicPartition)) {
                    Channel channel = getChannel(record.key().getChannelID());
                    channel.recordFromKafka(record);
                }

                for (Channel channel : channels.values()) {
                    channel.pollSubscribers();
                }
            }
        } catch (Exception e) {
            if (e instanceof WakeupException && shutdownRequested.get()) {
                // Graceful shutdown, not an error
            } else {
                threadFailed.set(true);
                startupFuture.completeExceptionally(e);
                log.error("Storage engine error", e);
            }
        }
    }

    public Future<RecordMetadata> publishEvent(ChannelKey key, byte[] value) {
        // TODO check for duplicates. use a queue and process on handler thread?
        ProducerRecord<ChannelKey, byte[]> record = new ProducerRecord<>(
                topicPartition.topic(), topicPartition.partition(), key, value);
        return producer.send(record);
    }

    public void subscribe(ClientConnection connection, String channelID, long startOffset) {
        if (!clientSubscriptions.containsKey(connection)) {
            clientSubscriptions.putIfAbsent(connection, ConcurrentHashMap.newKeySet());
        }
        clientSubscriptions.get(connection).add(channelID);

        getChannel(channelID).subscribe(connection, startOffset);
    }

    public void unsubscribe(ClientConnection connection) {
        Set<String> subscriptions = clientSubscriptions.remove(connection);
        if (subscriptions == null) return;

        for (String channelID : subscriptions) {
            getChannel(channelID).unsubscribe(connection);
        }
    }

    Channel getChannel(String channelID) {
        if (!channels.containsKey(channelID)) {
            channels.putIfAbsent(channelID, new Channel(channelID, messageStore));
        }
        return channels.get(channelID);
    }

    public boolean hasFailed() {
        return threadFailed.get();
    }
}
