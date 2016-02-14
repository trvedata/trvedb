package org.trvedata.trvedb.storage;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Provides typed access to a RocksDB column family (a subset of the key space).
 * Create a column family by calling {@link KeyValueStore#addColumnFamily(String, Serdes)}.
 * The methods for reading and writing from the store are thread-safe.
 */
public class ColumnFamily<K, V> {

    private final String name;
    private final Serdes<K, V> serdes;
    private volatile RocksDB db;
    private volatile ColumnFamilyHandle handle;

    /**
     * Create a new instance via {@link KeyValueStore#addColumnFamily(String, Serdes)}.
     */
    ColumnFamily(String name, Serdes<K, V> serdes) {
        this.name = name;
        this.serdes = serdes;
    }

    void setDBHandle(RocksDB db, ColumnFamilyHandle handle) {
        this.db = db;
        this.handle = handle;
    }

    /**
     * Looks up the value for the given key in the database. Returns null
     * if the key is not found.
     */
    public V get(K key) throws RocksDBException {
        byte[] keyBytes = serdes.getKeySerializer().serialize(name, key);
        byte[] valueBytes = db.get(handle, keyBytes);
        return serdes.getValueDeserializer().deserialize(name, valueBytes);
    }

    /**
     * Adds the given key-value pair to the database, overwriting any existing
     * value for the same key.
     */
    public void put(K key, V value) throws RocksDBException {
        byte[] keyBytes = serdes.getKeySerializer().serialize(name, key);
        byte[] valueBytes = serdes.getValueSerializer().serialize(name, value);
        db.put(handle, keyBytes, valueBytes);
    }

    /**
     * Returns the key-value pair in the database that most closely precedes the
     * given key (i.e. finds the entry with the greatest key that is less than
     * the given argument). Returns null if there is no such key.
     */
    public Pair<K, V> getBefore(K key) throws RocksDBException {
        byte[] keyBytes = serdes.getKeySerializer().serialize(name, key);
        RocksIterator it = null;
        try {
            it = db.newIterator(handle);
            it.seek(keyBytes);
            if (it.isValid()) it.prev(); else it.seekToLast();
            if (!it.isValid()) return null;

            return new Pair<K, V>(
                    serdes.getKeyDeserializer().deserialize(name, it.key()),
                    serdes.getValueDeserializer().deserialize(name, it.value())
            );
        } finally {
            if (it != null) it.dispose();
        }
    }

    public static class Pair<K, V> {
        private final K key;
        private final V value;

        Pair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }
}
