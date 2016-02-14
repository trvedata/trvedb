package org.trvedata.trvedb.storage;

import org.trvedata.trvedb.Encoding;
import org.trvedata.trvedb.avro.ChannelID;
import org.trvedata.trvedb.avro.SendMessage;
import org.trvedata.trvedb.avro.SendMessageError;
import org.trvedata.trvedb.avro.ServerToClient;

/**
 * Indicates that a {@link SendMessage} event from a client could not be processed
 * due to an inconsistency between client state and server state. If this happens,
 * an informational response is sent back to the client, and it is expected that
 * the client should be able to recover and retry gracefully.
 */
public class PublishException extends Exception {
    private static final long serialVersionUID = 1L;
    private final ChannelID channelID;
    private final long lastKnownSeqNo;

    public PublishException(ChannelID channelID, long lastKnownSeqNo) {
        super("Unexpected senderSeqNo in channel " + Encoding.channelID(channelID));
        this.channelID = channelID;
        this.lastKnownSeqNo = lastKnownSeqNo;
    }

    public ServerToClient messageToClient() {
        return new ServerToClient(new SendMessageError(channelID, lastKnownSeqNo));
    }
}
