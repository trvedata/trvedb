package org.trvedata.trvedb.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trvedata.trvedb.StreamKey;
import org.trvedata.trvedb.websocket.ClientConnection;

/**
 * Manages a single fine-grained stream and the subscribers on it. Everything
 * here runs on the {@link PartitionHandler} thread to which the stream belongs,
 * unless indicated otherwise.
 */
public class Stream {

    private static final Logger log = LoggerFactory.getLogger(Stream.class);
    private final ConcurrentMap<ClientConnection, Long> subscriberOffsets = new ConcurrentHashMap<>();
    private final String streamId;
    private final ColumnFamily<StreamKey, byte[]> messageStore;
    private final List<ConsumerRecord<StreamKey, byte[]>> records = new ArrayList<>();

    public Stream(String streamId, ColumnFamily<StreamKey, byte[]> messageStore) {
        this.streamId = streamId;
        this.messageStore = messageStore;
    }

    public String getStreamId() {
        return streamId;
    }

    /**
     * Receives an incoming message from the Kafka consumer.
     */
    public void recordFromKafka(ConsumerRecord<StreamKey, byte[]> record) throws RocksDBException {
        //String hex = String.format("%0" + (2*record.value().length) + "x", new BigInteger(1, record.value()));
        log.info("Received from Kafka: {} offset={}", record.key().toString(), record.offset());

        if (!records.isEmpty() && records.get(records.size() - 1).offset() >= record.offset()) {
            throw new IllegalArgumentException("Non-monotonic Kafka message offset: " +
                    records.get(records.size() - 1).offset() + " to " + record.offset());
        }
        records.add(record);
        messageStore.put(record.key(), record.value());
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
