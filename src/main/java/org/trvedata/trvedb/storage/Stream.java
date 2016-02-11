package org.trvedata.trvedb.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.xml.bind.DatatypeConverter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trvedata.trvedb.Encoding;
import org.trvedata.trvedb.StreamKey;
import org.trvedata.trvedb.avro.ChannelID;
import org.trvedata.trvedb.avro.ReceiveMessage;
import org.trvedata.trvedb.avro.ServerToClient;
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
    private final ChannelID avroStreamId;
    private final ColumnFamily<StreamKey, byte[]> messageStore;
    private final List<ConsumerRecord<StreamKey, byte[]>> records = new ArrayList<>();

    public Stream(String streamId, ColumnFamily<StreamKey, byte[]> messageStore) {
        this.streamId = streamId;
        this.avroStreamId = Encoding.channelID(streamId);
        this.messageStore = messageStore;
    }

    public String getStreamId() {
        return streamId;
    }

    /**
     * Receives an incoming message from the Kafka consumer.
     */
    public void recordFromKafka(ConsumerRecord<StreamKey, byte[]> record) throws RocksDBException {
        String hex = DatatypeConverter.printHexBinary(record.value()).toLowerCase();
        log.info("Received from Kafka: {} offset={} value={}", record.key().toString(), record.offset(), hex);

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

            // TODO this should be read from messageStore, not from an in-memory list in such a stupid way
            int i = 0;
            while (i < records.size() && records.get(i).offset() <= entry.getValue()) i++;
            if (i == records.size()) continue;

            ClientConnection connection = entry.getKey();
            while (i < records.size() && offerToClient(connection, records.get(i))) {
                log.info("Channel {}: sending offset to client {}", streamId,
                    records.get(i).offset(), connection.getSenderId());
                entry.setValue(records.get(i).offset());
                i++;
            }
        }
    }

    private boolean offerToClient(ClientConnection connection, ConsumerRecord<StreamKey, byte[]> record) {
        ReceiveMessage message = new ReceiveMessage(
            avroStreamId,
            Encoding.peerID(record.key().getSenderId()),
            (long) record.key().getSeqNo(),
            record.offset(),
            ByteBuffer.wrap(record.value()));

        return connection.offerMessage(new ServerToClient(message));
    }

    /**
     * Subscribes a WebSocket connection to receive messages from this stream.
     * This method is called by threads in the Jetty thread pool.
     */
    public void subscribe(ClientConnection connection, long startOffset) {
        log.info("Subscribed to channel {} with startOffset {}", streamId, startOffset);
        subscriberOffsets.putIfAbsent(connection, startOffset);
    }

    /**
     * Unsubscribes a WebSocket connection that has been closed.
     * This method is called by threads in the Jetty thread pool.
     */
    public void unsubscribe(ClientConnection connection) {
        log.info("Unsubscribed {} from channel {}", connection.getSenderId(), streamId);
        subscriberOffsets.remove(connection);
    }
}
