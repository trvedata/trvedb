package com.martinkl.logserver;

import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamStore {

    public static final String DEFAULT_BOOTSTRAP_SERVER = "localhost:9092";
    public static final String DEFAULT_KAFKA_TOPIC = "events";
    public static final int NUM_PARTITIONS = 16; // TODO make configurable
    private static final Logger log = LoggerFactory.getLogger(StreamStore.class);

    private final PartitionHandler[] handlers;
    private final Producer<StreamKey, byte[]> producer;

    public StreamStore() {
        this(null, null, 0, new int[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    }

    public StreamStore(String bootstrapServer, String kafkaTopic, int nodeId, int[] partitionToNode) {
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
            if (partitionToNode[i] == nodeId) {
                TopicPartition topicPartition = new TopicPartition(kafkaTopic, i);
                this.handlers[i] = new PartitionHandler(topicPartition, consumerConfig, producer);
            } else {
                throw new UnsupportedOperationException("TODO multi-node support");
            }
        }
    }

    public Future<RecordMetadata> publishEvent(StreamKey key, byte[] value) {
        PartitionHandler handler = handlers[key.getPartition()];
        return handler.publishEvent(key, value);
    }

    public void run() {
        Thread[] threads = new Thread[NUM_PARTITIONS];
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            threads[i] = new Thread(handlers[i]);
            threads[i].start();
        }
        log.info("Started handler threads for {} partitions.", NUM_PARTITIONS);

        for (int i = 0; i < NUM_PARTITIONS; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }
    }
}
