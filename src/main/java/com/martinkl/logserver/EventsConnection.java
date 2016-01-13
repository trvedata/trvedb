package com.martinkl.logserver;

import java.util.Arrays;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An EventsConnection instance is created for every established WebSocket connection
 * from a client. It receives callbacks when messages are received over the connection,
 * and provides a channel for the server to send messages to the client. 
 */
public class EventsConnection extends WebSocketAdapter {

    private static final Logger log = LoggerFactory.getLogger(EventsConnection.class);
    private final StreamStore store;
    private final String streamId;
    private final String senderId;
    private int seqNo;

    public EventsConnection(StreamStore store, String streamId, String senderId, int initialSeqNo) {
        this.store = store;
        this.streamId = streamId;
        this.senderId = senderId;
        this.seqNo = initialSeqNo;
    }

    @Override
    public void onWebSocketConnect(Session session) {
        super.onWebSocketConnect(session);
        log.info("Socket connected on stream {}: {}", streamId, session);
    }

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

    @Override
    public void onWebSocketText(String message) {
        super.onWebSocketText(message);
        log.info("Received TEXT message on stream {}: {}", streamId, message);
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        super.onWebSocketClose(statusCode, reason);
        log.info("Socket for stream {} closed: {}", streamId, Integer.toString(statusCode) + " " + reason);
    }

    @Override
    public void onWebSocketError(Throwable cause) {
        super.onWebSocketError(cause);
        log.info("Socket error: ", cause);
    }
}
