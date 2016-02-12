package org.trvedata.trvedb;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.trvedata.trvedb.storage.StreamStore;

public class ChannelKey {

    public static class KeySerializer implements Serializer<ChannelKey> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, ChannelKey data) {
            return (data == null) ? null : data.getEncoded();
        }

        @Override
        public void close() {
        }
    }

    public static class KeyDeserializer implements Deserializer<ChannelKey> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public ChannelKey deserialize(String topic, byte[] data) {
            return (data == null) ? null : new ChannelKey(data);
        }

        @Override
        public void close() {
        }
    }

    private byte[] encoded;
    private String channelID;
    private String senderID;
    private int seqNo;

    public ChannelKey(String channelID, String senderID, int seqNo) {
        this.encoded = null;
        this.channelID = channelID;
        this.senderID = senderID;
        this.seqNo = seqNo;
    }

    public ChannelKey(byte[] encoded) {
        this.encoded = encoded;
        this.channelID = null;
        this.senderID = null;
        this.seqNo = -1;
    }

    public byte[] getEncoded() {
        encode();
        return encoded;
    }

    public String getChannelID() {
        decode();
        return channelID;
    }

    public String getSenderID() {
        decode();
        return senderID;
    }

    public int getSeqNo() {
        decode();
        return seqNo;
    }

    public int getPartition() {
        encode();
        int firstByte = encoded[1];
        if (firstByte < 0) firstByte += 256;
        return firstByte % StreamStore.NUM_PARTITIONS;
    }

    private void encode() {
        if (this.encoded != null) return;

        ByteBuffer buf = ByteBuffer.allocate(53);
        buf.put((byte) 0); // version
        hexToByteBuffer(buf, channelID, 16);
        hexToByteBuffer(buf, senderID, 32);
        buf.putInt(seqNo);
        this.encoded = buf.array();
    }

    private void decode() {
        if (this.channelID != null) return;

        ByteBuffer buf = ByteBuffer.wrap(encoded);
        byte version = buf.get();
        if (version != 0) throw new IllegalStateException("Bad version number: " + version);

        this.channelID = byteBufferToHex(buf, 16);
        this.senderID = byteBufferToHex(buf, 32);
        this.seqNo = buf.getInt();
    }

    private void hexToByteBuffer(ByteBuffer output, String hex, int numBytes) {
        byte[] bytes = new BigInteger(hex, 16).toByteArray();
        if (bytes.length > numBytes) {
            output.put(bytes, bytes.length - numBytes, numBytes);
        } else {
            for (int i = bytes.length; i < numBytes; i++) output.put((byte) 0);
            output.put(bytes);
        }
    }

    private String byteBufferToHex(ByteBuffer input, int numBytes) {
        byte[] bytes = new byte[numBytes];
        input.get(bytes);
        return String.format("%0" + (numBytes * 2) + "x", new BigInteger(1, bytes));
    }

    @Override
    public int hashCode() {
        encode();
        return Arrays.hashCode(encoded);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (!(obj instanceof ChannelKey)) return false;
        encode();
        ChannelKey other = (ChannelKey) obj;
        if (!Arrays.equals(encoded, other.getEncoded())) return false;
        return true;
    }

    @Override
    public String toString() {
        decode();
        return "ChannelKey [channelID=" + channelID + ", senderID=" + senderID + ", seqNo=" + seqNo + "]";
    }
}
