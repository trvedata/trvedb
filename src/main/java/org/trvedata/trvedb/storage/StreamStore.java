package org.trvedata.trvedb.storage;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trvedata.trvedb.ChannelKey;
import org.trvedata.trvedb.websocket.ClientConnection;
import io.dropwizard.lifecycle.Managed;

public class StreamStore implements Managed {

    public static final String DEFAULT_BOOTSTRAP_SERVER = "localhost:9092";
    public static final String DEFAULT_KAFKA_TOPIC = "events";
    public static final int NUM_PARTITIONS = 16; // TODO make configurable
    public static final Path STORAGE_PATH = Paths.get("data").toAbsolutePath(); // TODO make configurable
    private static final Logger log = LoggerFactory.getLogger(StreamStore.class);

    private final PartitionHandler[] handlers;
    private final Producer<ChannelKey, byte[]> producer;

    public StreamStore() {
        this(null, null, 0, new int[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    }

    public StreamStore(String bootstrapServer, String kafkaTopic, int nodeID, int[] partitionToNode) {
        RocksDB.loadLibrary();

        if (bootstrapServer == null) bootstrapServer = DEFAULT_BOOTSTRAP_SERVER;
        Properties consumerConfig = PartitionHandler.consumerConfig();
        Properties producerConfig = PartitionHandler.producerConfig();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        producer = new KafkaProducer<>(producerConfig);

        if (kafkaTopic == null) kafkaTopic = DEFAULT_KAFKA_TOPIC;
        if (partitionToNode.length != NUM_PARTITIONS) {
            throw new IllegalArgumentException("Expected " + NUM_PARTITIONS + " partitions, got " +
                        partitionToNode.length);
        }

        this.handlers = new PartitionHandler[NUM_PARTITIONS];
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            if (partitionToNode[i] == nodeID) {
                TopicPartition topicPartition = new TopicPartition(kafkaTopic, i);
                this.handlers[i] = new PartitionHandler(topicPartition, consumerConfig, producer);
            } else {
                throw new UnsupportedOperationException("TODO multi-node support");
            }
        }
    }

    @Override
    public void start() throws Exception {
        for (int i = 0; i < NUM_PARTITIONS; i++) handlers[i].start();
        log.info("Started handler threads for {} partitions.", NUM_PARTITIONS);
    }

    @Override
    public void stop() throws Exception {
        for (int i = 0; i < NUM_PARTITIONS; i++) handlers[i].stop();
        producer.close(10, TimeUnit.SECONDS);
        for (int i = 0; i < NUM_PARTITIONS; i++) handlers[i].waitForShutdown();
    }

    public void publishEvent(ChannelKey key, byte[] value) throws PublishException {
        handlers[key.getPartition()].publishEvent(key, value);
    }

    public void subscribe(ClientConnection connection, String channelID, long startOffset) {
        ChannelKey key = new ChannelKey(channelID, connection.getPeerID(), 0);
        handlers[key.getPartition()].subscribe(connection, channelID, startOffset);
    }

    public void unsubscribe(ClientConnection connection) {
        for (int i = 0; i < NUM_PARTITIONS; i++) handlers[i].unsubscribe(connection);
    }
}
