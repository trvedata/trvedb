package org.trvedata.trvedb.storage;

import java.io.Closeable;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around a {@link RocksDB} key-value store.
 */
public class KeyValueStore implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KeyValueStore.class);
    private final String name;
    private final Path storagePath;
    private final Map<String, ColumnFamily<?, ?>> columnFamilies = new HashMap<>();
    private DBOptions options;
    private RocksDB db;
    private ColumnFamily<String, String> defaultColumnFamily;

    public KeyValueStore(Path storagePath) {
        this.name = storagePath.getFileName().toString();
        this.storagePath = storagePath;
    }

    /**
     * Registers a column family with the database. Each column family has its own
     * namespace of keys, and its own {@link Serdes}. This method must be called
     * before the database is opened. After the database is opened, all read/write
     * access to the database is done via the column family object.
     */
    public <K,V> ColumnFamily<K,V> addColumnFamily(String name, Serdes<K,V> serdes) {
        if (db != null) {
            throw new IllegalStateException("Cannot addColumnFamily after database is already open");
        }
        ColumnFamily<K,V> family = new ColumnFamily<>(name, serdes);
        columnFamilies.put(name, family);
        return family;
    }

    /**
     * Default column family that always exists.
     */
    public ColumnFamily<String, String> defaultColumnFamily() {
        return defaultColumnFamily;
    }

    public void open() throws RocksDBException {
        if (db != null) throw new IllegalStateException("Database already open");
        storagePath.toFile().mkdirs();
        defaultColumnFamily = addColumnFamily("default", Serdes.stringSerdes()); // must exist

        options = new DBOptions();
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        //options.setBytesPerSync(1024 * 1024);

        List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        List<ColumnFamily<?, ?>> families = new ArrayList<>();
        try {
            for (Map.Entry<String, ColumnFamily<?, ?>> entry : columnFamilies.entrySet()) {
                descriptors.add(new ColumnFamilyDescriptor(entry.getKey().getBytes("UTF-8")));
                families.add(entry.getValue());
            }
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        db = RocksDB.open(options, storagePath.toString(), descriptors, handles);
        if (families.size() != handles.size()) {
            throw new IllegalStateException("Unexpected number of column family handles");
        }
        for (int i = 0; i < families.size(); i++) {
            families.get(i).setDBHandle(db, handles.get(i));
        }
        log.info("Opened database {} at path {}", name, storagePath);
    }

    @Override
    public void close() {
        if (db != null) {
            db.close();
            options.dispose();
            db = null;
            options = null;
            log.info("Closed database at path {}", storagePath);
        }
    }
}
