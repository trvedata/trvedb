package org.trvedata.trvedb.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import org.trvedata.trvedb.ChannelKey;
import org.trvedata.trvedb.avro.ChannelID;
import org.trvedata.trvedb.avro.ReceiveMessage;
import org.trvedata.trvedb.avro.ServerToClient;
import org.trvedata.trvedb.websocket.ClientConnection;

/**
 * Manages a single fine-grained channel and the subscribers on it. Methods
 * must be thread-safe unless indicated otherwise.
 */
public class Channel {

    private static final Logger log = LoggerFactory.getLogger(Channel.class);
    private final ConcurrentMap<ClientConnection, Long> subscriberOffsets = new ConcurrentHashMap<>();
    private final String channelID;
    private final ChannelID avroChannelID;
    private final PartitionHandler partition;
    private final ColumnFamily<ChannelKey, byte[]> messagesByChannelKey;

    // The following fields are protected by publishLock
    private final Object publishLock = new Object();
    private final Map<ChannelKey, byte[]> messagesInFlight = new HashMap<>();
    private final Map<String, Long> seqNoBySenderID = new HashMap<>();

    private final List<ConsumerRecord<ChannelKey, byte[]>> records = new ArrayList<>();

    public Channel(String channelID, PartitionHandler partition,
                   ColumnFamily<ChannelKey, byte[]> messageStore) {
        this.channelID = channelID;
        this.avroChannelID = Encoding.channelID(channelID);
        this.partition = partition;
        this.messagesByChannelKey = messageStore;
    }

    public String getChannelID() {
        return channelID;
    }

    /**
     * Receives an incoming message from the Kafka consumer. Must be called only by
     * the {@link PartitionHandler} thread to which the channel belongs.
     */
    void recordFromKafka(ConsumerRecord<ChannelKey, byte[]> record) throws RocksDBException {
        String hex = DatatypeConverter.printHexBinary(record.value()).toLowerCase();
        log.info("Received from Kafka: {} offset={} value={}", record.key().toString(), record.offset(), hex);

        if (!records.isEmpty() && records.get(records.size() - 1).offset() >= record.offset()) {
            throw new IllegalArgumentException("Non-monotonic Kafka message offset: " +
                    records.get(records.size() - 1).offset() + " to " + record.offset());
        }
        records.add(record);
        messagesByChannelKey.put(record.key(), record.value());

        synchronized (publishLock) {
            messagesInFlight.remove(record.key());
        }
    }

    /**
     * Checks whether any subscribers need to be fed messages. Must be called only by
     * the {@link PartitionHandler} thread to which the channel belongs.
     */
    void pollSubscribers() {
        if (records.isEmpty()) return;
        long latestOffset = records.get(records.size() - 1).offset();

        for (Map.Entry<ClientConnection, Long> entry : subscriberOffsets.entrySet()) {
            Long clientOffset = entry.getValue();
            if (clientOffset >= latestOffset) continue;

            // TODO this should be read from messageStore, not from an in-memory list in such a stupid way
            int i = 0;
            while (i < records.size() && records.get(i).offset() <= entry.getValue()) i++;
            if (i == records.size()) continue;

            ClientConnection connection = entry.getKey();
            while (i < records.size() && offerToClient(connection, records.get(i))) {
                log.info("Channel {}: sending offset {} to client {}", channelID,
                    records.get(i).offset(), connection.getPeerID());
                entry.setValue(records.get(i).offset());
                i++;
            }
        }
    }

    private boolean offerToClient(ClientConnection connection, ConsumerRecord<ChannelKey, byte[]> record) {
        ReceiveMessage message = new ReceiveMessage(
            avroChannelID,
            Encoding.peerID(record.key().getSenderID()),
            (long) record.key().getSeqNo(),
            record.offset(),
            ByteBuffer.wrap(record.value()));

        return connection.offerMessage(new ServerToClient(message));
    }

    /**
     * Called by Jetty threads when a request to send a message arrives from a
     * WebSocket client. Checks that the request makes sense before actually
     * sending it to Kafka.
     */
    public void publishEvent(ChannelKey key, byte[] value) throws PublishException, RocksDBException {
        if (!key.getChannelID().equals(channelID)) {
            throw new IllegalArgumentException("Publishing message to the wrong channel");
        }
        if (key.getSeqNo() <= 0) {
            throw new IllegalArgumentException("Sequence number must be positive");
        }

        synchronized (publishLock) {
            if (!checkSenderSeqNo(key, value)) return;
            messagesInFlight.put(key, value);
            seqNoBySenderID.put(key.getSenderID(), (long) key.getSeqNo());
        }

        partition.sendToKafka(key, value);
    }

    /**
     * Returns true if the message should be published, and false if it should be
     * discarded (because it is a harmless duplicate). Throws {@link PublishException}
     * if the sequence number jumped forwards or backwards.
     */
    private boolean checkSenderSeqNo(ChannelKey key, byte[] value) throws PublishException, RocksDBException {
        synchronized (publishLock) {
            ColumnFamily.Pair<ChannelKey, byte[]> lastMsg = getLastMessage(key.getSenderID());
            long lastSeqNo = (lastMsg == null) ? 0 : lastMsg.getKey().getSeqNo();

            if (key.getSeqNo() > lastSeqNo + 1) {
                throw new PublishException(avroChannelID, lastSeqNo);
            }

            if (key.getSeqNo() <= lastSeqNo) {
                byte[] previousPayload;
                if (key.getSeqNo() == lastSeqNo) {
                    previousPayload = lastMsg.getValue();
                } else {
                    previousPayload = getSeqNoMessage(key.getSenderID(), key.getSeqNo());
                }

                if (Arrays.equals(value, previousPayload)) {
                    log.info("Ignoring duplicate message for {}", key);
                    return false;
                } else {
                    throw new PublishException(avroChannelID, lastSeqNo);
                }
            }

            // Get here if key.getSeqNo() == lastMsg.getKey().getSeqNo() + 1
            return true;
        }
    }

    /**
     * Returns the most recent message we have seen for a particular senderID in
     * this channel. Returns null if no messages have been seen for that senderID.
     */
    private ColumnFamily.Pair<ChannelKey, byte[]> getLastMessage(String senderID) throws RocksDBException {
        synchronized (publishLock) {
            ChannelKey lastKey = null;
            Long lastSeqNo = seqNoBySenderID.get(senderID);

            if (lastSeqNo != null) {
                lastKey = new ChannelKey(channelID, senderID, lastSeqNo.intValue());
                byte[] inFlightMsg = messagesInFlight.get(lastKey);
                if (inFlightMsg != null) {
                    return new ColumnFamily.Pair<>(lastKey, inFlightMsg);
                }

                byte[] storedMsg = messagesByChannelKey.get(lastKey);
                if (storedMsg == null) {
                    throw new IllegalStateException("Message for last requested seqNo not found: " + lastKey);
                }
                return new ColumnFamily.Pair<>(lastKey, storedMsg);
            }

            ChannelKey upperBound = new ChannelKey(channelID, senderID, Integer.MAX_VALUE);
            ColumnFamily.Pair<ChannelKey, byte[]> lastMsg = messagesByChannelKey.getBefore(upperBound);

            if (lastMsg != null &&
                    lastMsg.getKey().getChannelID().equals(channelID) &&
                    lastMsg.getKey().getSenderID().equals(senderID)) {
                seqNoBySenderID.put(senderID, (long) lastMsg.getKey().getSeqNo());
                return lastMsg;
            } else {
                return null;
            }
        }
    }

    /**
     * Returns the message with a particular senderID and seqNo in this channel
     * (which is expected to be unique). Returns null if there is no such message.
     */
    private byte[] getSeqNoMessage(String senderID, long seqNo) throws RocksDBException {
        synchronized (publishLock) {
            ChannelKey key = new ChannelKey(channelID, senderID, (int) seqNo);
            byte[] inFlightMsg = messagesInFlight.get(key);

            if (inFlightMsg != null) {
                return inFlightMsg;
            } else {
                return messagesByChannelKey.get(key);
            }
        }
    }

    /**
     * Subscribes a WebSocket connection to receive messages from this channel.
     * This method is called by threads in the Jetty thread pool.
     */
    public void subscribe(ClientConnection connection, long startOffset) {
        log.info("Subscribed to channel {} with startOffset {}", channelID, startOffset);
        subscriberOffsets.putIfAbsent(connection, startOffset);
    }

    /**
     * Unsubscribes a WebSocket connection that has been closed.
     * This method is called by threads in the Jetty thread pool.
     */
    public void unsubscribe(ClientConnection connection) {
        log.info("Unsubscribed {} from channel {}", connection.getPeerID(), channelID);
        subscriberOffsets.remove(connection);

        synchronized (publishLock) {
            seqNoBySenderID.remove(connection.getPeerID());
        }
    }
}
