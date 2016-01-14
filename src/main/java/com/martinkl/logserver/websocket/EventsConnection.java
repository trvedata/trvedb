package com.martinkl.logserver.websocket;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.martinkl.logserver.StreamKey;
import com.martinkl.logserver.StreamStore;

/**
 * An EventsConnection instance is created for every established WebSocket connection
 * from a client. It receives callbacks when messages are received over the connection,
 * and provides a channel for the server to send messages to the client. 
 */
public class EventsConnection extends WebSocketAdapter {

    public static final int MAX_IN_FLIGHT_MESSAGES = 100;

    private static final Logger log = LoggerFactory.getLogger(EventsConnection.class);
    private final StreamStore store;
    private final ConnectionHandle handle;
    private final String streamId;
    private final String senderId;
    private int seqNo;

    public EventsConnection(StreamStore store, String streamId, String senderId, int initialSeqNo) {
        this.store = store;
        this.handle = new ConnectionHandle();
        this.streamId = streamId;
        this.senderId = senderId;
        this.seqNo = initialSeqNo;
    }

    /**
     * Called by Jetty when the WebSocket connection is established.
     */
    @Override
    public void onWebSocketConnect(Session session) {
        super.onWebSocketConnect(session);
        log.info("Socket connected on stream {}: {}", streamId, session);
        store.subscribe(handle);
    }

    /**
     * Called by Jetty when a binary frame is received from the WebSocket client.
     */
    @Override
    public void onWebSocketBinary(byte[] payload, int offset, int len) {
        super.onWebSocketBinary(payload, offset, len);
        StreamKey key = new StreamKey(streamId, senderId, seqNo);
        seqNo++;

        StringBuilder str = new StringBuilder("Received BINARY message: ");
        str.append(key);
        str.append(" value =");
        for (int i = 0; i < len; i++) {
            str.append(' ');
            int val = payload[offset + i];
            if (val < 0) val += 256; // signed to unsigned
            if (val < 16) str.append('0');
            str.append(Integer.toHexString(val));
        }
        log.info(str.toString());

        store.publishEvent(key, Arrays.copyOfRange(payload, offset, offset + len));

        getSession().getRemote().sendString(str.toString(), new WriteCallback() {
            @Override
            public void writeSuccess() {}

            @Override
            public void writeFailed(Throwable error) {
                log.info("Sending message failed: ", error);
            }
        });
    }

    /**
     * Called by Jetty when a text frame is received from the WebSocket client.
     */
    @Override
    public void onWebSocketText(String message) {
        super.onWebSocketText(message);
        log.info("Received TEXT message on stream {}: {}", streamId, message);
    }

    /**
     * Called by Jetty when the WebSocket connection is gracefully closed due
     * to a 'close' event.
     */
    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        super.onWebSocketClose(statusCode, reason);
        log.info("Socket for stream {} closed: {}", streamId, Integer.toString(statusCode) + " " + reason);
        store.unsubscribe(handle);
    }

    /**
     * Called by Jetty when the WebSocket connection is forcibly closed due to
     * an error.
     */
    @Override
    public void onWebSocketError(Throwable cause) {
        super.onWebSocketError(cause);
        log.info("Socket error: ", cause);
        store.unsubscribe(handle);
    }


    /**
     * The handle is given to the storage layer in order to subscribe to
     * streams. Its methods may be called by any thread.
     */
    private class ConnectionHandle implements ClientConnection {
        private AtomicInteger inFlightMessages = new AtomicInteger(0);

        public String getStreamId() {
            return streamId;
        }

        public String getSenderId() {
            return senderId;
        }

        public int getSeqNo() {
            return seqNo;
        }

        public StreamKey getStreamKey() {
            return new StreamKey(streamId, senderId, seqNo);
        }

        /**
         * @see ClientConnection#offerMessage(byte[])
         */
        public boolean offerMessage(byte[] message) {
            Session session = getSession();
            if (session == null || !session.isOpen()) return false;

            if (inFlightMessages.incrementAndGet() > MAX_IN_FLIGHT_MESSAGES) {
                inFlightMessages.decrementAndGet();
                return false;
            }

            session.getRemote().sendBytes(ByteBuffer.wrap(message), new WriteCallback() {
                @Override
                public void writeSuccess() {
                    inFlightMessages.decrementAndGet();
                }

                @Override
                public void writeFailed(Throwable error) {
                    inFlightMessages.decrementAndGet();
                    log.info("Sending message to WebSocket client failed: ", error);
                }
            });

            return true;
        }
    }
}
