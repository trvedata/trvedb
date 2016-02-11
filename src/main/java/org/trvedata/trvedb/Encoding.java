package org.trvedata.trvedb;

import javax.xml.bind.DatatypeConverter;
import org.trvedata.trvedb.avro.ChannelID;
import org.trvedata.trvedb.avro.PeerID;

public class Encoding {
    public static ChannelID channelID(String hexChannelID) {
        if (hexChannelID.length() != 32) {
            throw new IllegalArgumentException("ChannelID must be 128-bit hex");
        }
        return new ChannelID(DatatypeConverter.parseHexBinary(hexChannelID));
    }

    public static String channelID(ChannelID channelID) {
        return DatatypeConverter.printHexBinary(channelID.bytes()).toLowerCase();
    }

    public static PeerID peerID(String hexPeerID) {
        if (hexPeerID.length() != 64) {
            throw new IllegalArgumentException("PeerID must be 256-bit hex");
        }
        return new PeerID(DatatypeConverter.parseHexBinary(hexPeerID));
    }

    public static String peerID(PeerID peerID) {
        return DatatypeConverter.printHexBinary(peerID.bytes()).toLowerCase();
    }
}
