package org.trvedata.trvedb;

import static org.junit.Assert.*;
import java.util.Arrays;
import org.junit.Test;

public class TestStreamKey {
    @Test
    public void testAllZeros() {
        StreamKey example = new StreamKey("00000000000000000000000000000000", "0000000000000000", 0);
        assertArrayEquals(new byte[29], example.getEncoded());
        StreamKey parsed = new StreamKey(example.getEncoded());
        assertEquals("00000000000000000000000000000000", parsed.getStreamId());
        assertEquals("0000000000000000", parsed.getSenderId());
        assertEquals(0, parsed.getSeqNo());
    }

    @Test
    public void testAllOnes() {
        StreamKey example = new StreamKey("ffffffffffffffffffffffffffffffff", "ffffffffffffffff", Integer.MAX_VALUE);

        byte[] expected = new byte[29];
        for (int i = 1; i < 29; i++) expected[i] = -1;
        expected[25] = 127;
        assertArrayEquals(expected, example.getEncoded());

        StreamKey parsed = new StreamKey(example.getEncoded());
        assertEquals("ffffffffffffffffffffffffffffffff", parsed.getStreamId());
        assertEquals("ffffffffffffffff", parsed.getSenderId());
        assertEquals(Integer.MAX_VALUE, parsed.getSeqNo());
    }

    @Test
    public void testOtherValues() {
        StreamKey example = new StreamKey("00112233445566778899aabbccddeeff", "fedcba9876543210", 123456789);
        StreamKey parsed = new StreamKey(example.getEncoded());
        assertEquals("00112233445566778899aabbccddeeff", parsed.getStreamId());
        assertEquals("fedcba9876543210", parsed.getSenderId());
        assertEquals(123456789, parsed.getSeqNo());
    }

    @Test
    public void testEquality() {
        StreamKey example1 = new StreamKey("00112233445566778899aabbccddeeff", "0123456789abcdef", 123456789);
        StreamKey example2 = new StreamKey("00112233445566778899AABBCCDDEEFF", "0123456789ABCDEF", 123456789);
        StreamKey example3 = new StreamKey(Arrays.copyOf(example1.getEncoded(), 29));
        assertTrue(example1.equals(example2));
        assertTrue(example1.equals(example3));
        assertEquals(example1.hashCode(), example2.hashCode());
        assertEquals(example1.hashCode(), example3.hashCode());

        assertFalse(example1.equals(new StreamKey("00112233445566778899aabbccddeef0", "0123456789abcdef", 123456789)));
        assertFalse(example1.equals(new StreamKey("00112233445566778899aabbccddeeff", "0123456789abcde0", 123456789)));
        assertFalse(example1.equals(new StreamKey("00112233445566778899aabbccddeeff", "0123456789abcdef", 123456780)));
        assertFalse(example1.equals(null));
        assertFalse(example1.equals("nonsense"));
    }

    @Test
    public void testPartitionAssignment() {
        assertEquals(0,  new StreamKey("00112233445566778899aabbccddeeff", "0123456789abcdef", 123456789).getPartition());
        assertEquals(1,  new StreamKey("01112233445566778899aabbccddeeff", "0123456789abcdef", 123456789).getPartition());
        assertEquals(15, new StreamKey("0f112233445566778899aabbccddeeff", "0123456789abcdef", 123456789).getPartition());
        assertEquals(0,  new StreamKey("10112233445566778899aabbccddeeff", "0123456789abcdef", 123456789).getPartition());
    }
}
