package com.martinkl.logserver;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class StreamKey {

    public static class KeySerializer implements Serializer<StreamKey> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, StreamKey data) {
            return data.getEncoded();
        }

        @Override
        public void close() {
        }
    }

    public static class KeyDeserializer implements Deserializer<StreamKey> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public StreamKey deserialize(String topic, byte[] data) {
            return new StreamKey(data);
        }

        @Override
        public void close() {
        }
    }

    private byte[] encoded;
    private String streamId;
    private String senderId;
    private int seqNo;

    public StreamKey(String streamId, String senderId, int seqNo) {
        this.encoded = null;
        this.streamId = streamId;
        this.senderId = senderId;
        this.seqNo = seqNo;
    }

    public StreamKey(byte[] encoded) {
        this.encoded = encoded;
        this.streamId = null;
        this.senderId = null;
        this.seqNo = -1;
    }

    public byte[] getEncoded() {
        encode();
        return encoded;
    }

    public String getStreamId() {
        decode();
        return streamId;
    }

    public String getSenderId() {
        decode();
        return senderId;
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

        ByteBuffer buf = ByteBuffer.allocate(29);
        buf.put((byte) 0); // version
        hexToByteBuffer(buf, streamId, 16);
        hexToByteBuffer(buf, senderId, 8);
        buf.putInt(seqNo);
        this.encoded = buf.array();
    }

    private void decode() {
        if (this.streamId != null) return;

        ByteBuffer buf = ByteBuffer.wrap(encoded);
        byte version = buf.get();
        if (version != 0) throw new IllegalStateException("Bad version number: " + version);

        this.streamId = byteBufferToHex(buf, 16);
        this.senderId = byteBufferToHex(buf, 8);
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
        if (!(obj instanceof StreamKey)) return false;
        encode();
        StreamKey other = (StreamKey) obj;
        if (!Arrays.equals(encoded, other.getEncoded())) return false;
        return true;
    }

    @Override
    public String toString() {
        decode();
        return "StreamKey [streamId=" + streamId + ", senderId=" + senderId + ", seqNo=" + seqNo + "]";
    }
}
