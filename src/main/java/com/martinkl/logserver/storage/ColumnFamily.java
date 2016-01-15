package com.martinkl.logserver.storage;

import org.rocksdb.RocksDBException;

public class ColumnFamily<K, V> {

    private final KeyValueStore store;
    private final String name;
    private final Serdes<K, V> serdes;

    /**
     * Create a new instance via {@link KeyValueStore#addColumnFamily(String, Serdes)}.
     */
    ColumnFamily(KeyValueStore store, String name, Serdes<K, V> serdes) {
        this.store = store;
        this.name = name;
        this.serdes = serdes;
    }

    public V get(K key) throws RocksDBException {
        byte[] keyBytes = serdes.getKeySerializer().serialize(name, key);
        byte[] valueBytes = store.get(this, keyBytes);
        return serdes.getValueDeserializer().deserialize(name, valueBytes);
    }

    public void put(K key, V value) throws RocksDBException {
        byte[] keyBytes = serdes.getKeySerializer().serialize(name, key);
        byte[] valueBytes = serdes.getValueSerializer().serialize(name, value);
        store.put(this, keyBytes, valueBytes);
    }
}
