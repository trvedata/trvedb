package org.trvedata.trvedb.websocket;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trvedata.trvedb.Encoding;
import org.trvedata.trvedb.ChannelKey;
import org.trvedata.trvedb.avro.ClientToServer;
import org.trvedata.trvedb.avro.SendMessage;
import org.trvedata.trvedb.avro.ServerToClient;
import org.trvedata.trvedb.avro.SubscribeToChannel;
import org.trvedata.trvedb.storage.PublishException;
import org.trvedata.trvedb.storage.StreamStore;

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
    private final String peerID;
    private final DatumReader<ClientToServer> readFromClient = new SpecificDatumReader<>(ClientToServer.class);
    private final DatumWriter<ServerToClient> writeToClient = new SpecificDatumWriter<>(ServerToClient.class);

    public EventsConnection(StreamStore store, String peerID) {
        this.store = store;
        this.handle = new ConnectionHandle();
        this.peerID = peerID;
    }

    /**
     * Called by Jetty when the WebSocket connection is established.
     */
    @Override
    public void onWebSocketConnect(Session session) {
        super.onWebSocketConnect(session);
        log.info("Connection from peer {}", peerID);
    }

    /**
     * Called by Jetty when a binary frame is received from the WebSocket client.
     */
    @Override
    public void onWebSocketBinary(byte[] payload, int offset, int len) {
        Session session = getSession();
        if (session == null || !session.isOpen()) return;

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, offset, len, null);
        Object request;
        try {
            request = readFromClient.read(null, decoder).getMessage();
        } catch (Exception e) {
            log.warn("Failed to decode message from client", e);
            session.close(StatusCode.BAD_PAYLOAD, "Could not parse frame");
            return;
        }

        try {
            if (request instanceof SendMessage) {
                sendMessageRequest((SendMessage) request);
            } else if (request instanceof SubscribeToChannel) {
                subscribeRequest((SubscribeToChannel) request);
            } else {
                throw new IllegalStateException("Unknown request type: " + request.getClass().getName());
            }
        } catch (Exception e) {
            log.info("Failed to publish message", e);
            session.close(StatusCode.SERVER_ERROR, "Internal server error");
        }
    }

    /**
     * Called by Jetty when a text frame is received from the WebSocket client.
     */
    @Override
    public void onWebSocketText(String message) {
        log.info("Received unsupported text message from peer {}", peerID);
        Session session = getSession();
        if (session != null && session.isOpen()) {
            session.close(StatusCode.BAD_DATA, "This endpoint only accepts binary messages");
        }
    }

    /**
     * Called by Jetty when the WebSocket connection is gracefully closed due
     * to a 'close' event.
     */
    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        super.onWebSocketClose(statusCode, reason);
        log.info("Socket for peer {} closed: {}", peerID, Integer.toString(statusCode) + " " + reason);
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
     * Handles a client request to send a message to a channel.
     */
    private void sendMessageRequest(SendMessage send) {
        Session session = getSession();
        if (session == null || !session.isOpen()) return;

        try {
            ChannelKey key = new ChannelKey(Encoding.channelID(send.getChannelID()),
                peerID, send.getSenderSeqNo().intValue());
            store.publishEvent(key, send.getPayload().array());

        } catch (PublishException e) {
            log.info("SendMessage request refused: {}", e.toString());
            if (!handle.offerMessage(e.messageToClient())) {
                session.close(StatusCode.SERVER_ERROR, "Internal server error");
            }

        } catch (TimeoutException | BufferExhaustedException e) {
            // The Kafka producer has a fixed-size buffer of messages being sent. Normally,
            // if buffer space is available, the send request returns immediately and is
            // processed asynchronously. If clients send messages faster than they can be
            // published to Kafka, the buffer space fills up, and send requests start
            // blocking. The maximum blocking time is configured to be fairly short. If it
            // is exceeded, TimeoutException is thrown to indicate that the producer is
            // overloaded. To shed load, close connection and ask the client to back off.
            log.warn("Kafka producer buffer is full", e);
            session.close(StatusCode.TRY_AGAIN_LATER, "SendMessage request failed due to server overload");
        }
    }

    /**
     * Handles a client request to subscribe to a channel.
     */
    private void subscribeRequest(SubscribeToChannel sub) {
        store.subscribe(handle, Encoding.channelID(sub.getChannelID()), sub.getStartOffset());
    }

    /**
     * The handle is given to the storage layer in order to subscribe to
     * channels. Its methods may be called by any thread.
     */
    private class ConnectionHandle implements ClientConnection {
        private AtomicInteger inFlightMessages = new AtomicInteger(0);

        public String getPeerID() {
            return peerID;
        }

        /**
         * @see ClientConnection#offerMessage(ServerToClient)
         */
        public boolean offerMessage(ServerToClient message) {
            Session session = getSession();
            if (session == null || !session.isOpen()) return false;

            if (inFlightMessages.incrementAndGet() > MAX_IN_FLIGHT_MESSAGES) {
                inFlightMessages.decrementAndGet();
                return false;
            }

            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            try {
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
                writeToClient.write(message, encoder);
                encoder.flush();
            } catch (Exception e) {
                log.warn("Failed to encode message to client", e);
                session.close(StatusCode.SERVER_ERROR, "Internal server error");
                return true;
            }

            session.getRemote().sendBytes(ByteBuffer.wrap(stream.toByteArray()), new WriteCallback() {
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
