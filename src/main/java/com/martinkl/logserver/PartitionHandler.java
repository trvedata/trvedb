package com.martinkl.logserver;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
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
import com.martinkl.logserver.websocket.ClientConnection;

public class PartitionHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(PartitionHandler.class);
    private final TopicPartition topicPartition;
    private final Consumer<StreamKey, byte[]> consumer;
    private final Producer<StreamKey, byte[]> producer;
    private final ConcurrentMap<String, Set<ClientConnection>> subscribers = new ConcurrentHashMap<>();
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    public PartitionHandler(TopicPartition topicPartition, Properties consumerConfig,
                            Producer<StreamKey, byte[]> producer) {
        this.topicPartition = topicPartition;
        this.consumer = new KafkaConsumer<>(consumerConfig);
        this.producer = producer;
    }

    public static Properties consumerConfig() {
        Properties config = new Properties();
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StreamKey.KeyDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return config;
    }

    public static Properties producerConfig() {
        Properties config = new Properties();
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StreamKey.KeySerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return config;
    }

    @Override
    public void run() {
        try {
            consumer.assign(Arrays.asList(topicPartition));
            consumer.seekToBeginning(topicPartition); // TODO resume from last offset

            while (!shutdownRequested.get()) {
                ConsumerRecords<StreamKey, byte[]> records = consumer.poll(10000);
                for (ConsumerRecord<StreamKey, byte[]> record : records.records(topicPartition)) {
                    recordFromKafka(record);
                }
            }
        } catch (WakeupException e) {
            if (!shutdownRequested.get()) throw e;
        } finally {
            consumer.close();
        }
    }

    public Future<RecordMetadata> publishEvent(StreamKey key, byte[] value) {
        // TODO check for duplicates. use a queue and process on handler thread?
        ProducerRecord<StreamKey, byte[]> record = new ProducerRecord<>(
                topicPartition.topic(), topicPartition.partition(), key, value);
        return producer.send(record);
    }

    public void subscribe(ClientConnection connection) {
        String streamId = connection.getStreamId();
        if (!subscribers.containsKey(streamId)) {
            subscribers.putIfAbsent(streamId, ConcurrentHashMap.newKeySet());
        }
        subscribers.get(streamId).add(connection);
    }

    public void unsubscribe(ClientConnection connection) {
        String streamId = connection.getStreamId();
        if (subscribers.containsKey(streamId)) {
            subscribers.get(streamId).remove(connection);
        }
    }

    private void recordFromKafka(ConsumerRecord<StreamKey, byte[]> record) {
        String hex = String.format("%0" + (2*record.value().length) + "x", new BigInteger(1, record.value()));
        log.info("Received from Kafka: {} value={}", record.key().toString(), hex);
    }

    public void shutdown() {
        shutdownRequested.set(true);
        consumer.wakeup();
    }
}
