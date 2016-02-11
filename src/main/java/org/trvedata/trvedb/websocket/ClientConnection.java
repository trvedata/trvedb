package org.trvedata.trvedb.websocket;

import org.trvedata.trvedb.avro.ServerToClient;

/**
 * A handle to a WebSocket connection with a client. This handle is passed to
 * the storage layer in order to subscribe to streams.
 */
public interface ClientConnection {
    String getSenderId();

    /**
     * Sends a message to the WebSocket client. This method must be
     * called only by a single thread in the storage layer, so it need not
     * be thread-safe. Does not block, but instead returns false if the
     * connection is busy and the caller should retry later.
     *
     * @param message The message to send to the WebSocket client (will be Avro-encoded).
     * @return true if message was accepted for sending, false if the buffer is full.
     */
    boolean offerMessage(ServerToClient message);
}
