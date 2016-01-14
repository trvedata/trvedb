package com.martinkl.logserver.storage;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.martinkl.logserver.StreamKey;
import com.martinkl.logserver.websocket.ClientConnection;

/**
 * Manages a single fine-grained stream and the subscribers on it. Everything
 * here runs on the {@link PartitionHandler} thread to which the stream belongs,
 * unless indicated otherwise.
 */
public class Stream {

    private static final Logger log = LoggerFactory.getLogger(Stream.class);
    private final ConcurrentMap<ClientConnection, Long> subscriberOffsets = new ConcurrentHashMap<>();
    private final String streamId;
    private final List<ConsumerRecord<StreamKey, byte[]>> records = new ArrayList<>();

    public Stream(String streamId) {
        this.streamId = streamId;
    }

    public String getStreamId() {
        return streamId;
    }

    /**
     * Receives an incoming message from the Kafka consumer.
     */
    public void recordFromKafka(ConsumerRecord<StreamKey, byte[]> record) {
        //String hex = String.format("%0" + (2*record.value().length) + "x", new BigInteger(1, record.value()));
        log.info("Received from Kafka: {} offset={}", record.key().toString(), record.offset());

        if (!records.isEmpty() && records.get(records.size() - 1).offset() >= record.offset()) {
            throw new IllegalArgumentException("Non-monotonic Kafka message offset: " +
                    records.get(records.size() - 1).offset() + " to " + record.offset());
        }
        records.add(record);
    }

    /**
     * Checks whether any subscribers need to be fed messages.
     */
    public void pollSubscribers() {
        if (records.isEmpty()) return;
        long latestStreamOffset = records.get(records.size() - 1).offset();

        for (Map.Entry<ClientConnection, Long> entry : subscriberOffsets.entrySet()) {
            Long clientOffset = entry.getValue();
            if (clientOffset >= latestStreamOffset) continue;

            log.info("Poll: offset {} -> {}", clientOffset, latestStreamOffset);

            int i = 0;
            while (i < records.size() && records.get(i).offset() <= entry.getValue()) i++;
            if (i == records.size()) continue;

            ClientConnection connection = entry.getKey();
            while (i < records.size() && connection.offerMessage(records.get(i).value())) {
                entry.setValue(records.get(i).offset());
                i++;
            }
        }
    }

    /**
     * Subscribes a WebSocket connection to receive messages from this stream.
     * This method is called by threads in the Jetty thread pool.
     */
    public void subscribe(ClientConnection connection) {
        if (!subscriberOffsets.containsKey(connection)) {
            subscriberOffsets.putIfAbsent(connection, 0L);
        }
    }

    /**
     * Unsubscribes a WebSocket connection that has been closed.
     * This method is called by threads in the Jetty thread pool.
     */
    public void unsubscribe(ClientConnection connection) {
        subscriberOffsets.remove(connection);
    }
}
