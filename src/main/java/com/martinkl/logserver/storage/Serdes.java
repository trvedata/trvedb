package com.martinkl.logserver.storage;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Serdes<K, V> {

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    public Serdes(Serializer<K> keySerializer, Deserializer<K> keyDeserializer,
                  Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) {
        this.keySerializer = keySerializer;
        this.keyDeserializer = keyDeserializer;
        this.valueSerializer = valueSerializer;
        this.valueDeserializer = valueDeserializer;
    }

    public Serializer<K> getKeySerializer() {
        return keySerializer;
    }

    public Serializer<V> getValueSerializer() {
        return valueSerializer;
    }

    public Deserializer<K> getKeyDeserializer() {
        return keyDeserializer;
    }

    public Deserializer<V> getValueDeserializer() {
        return valueDeserializer;
    }

    public static Serdes<byte[], byte[]> byteArraySerdes() {
        return new Serdes<>(
                new ByteArraySerializer(), new ByteArrayDeserializer(),
                new ByteArraySerializer(), new ByteArrayDeserializer());
    }

    public static Serdes<String, String> stringSerdes() {
        return new Serdes<>(
                new StringSerializer(), new StringDeserializer(),
                new StringSerializer(), new StringDeserializer());
    }
}
