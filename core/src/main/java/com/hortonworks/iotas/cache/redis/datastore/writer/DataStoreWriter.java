package com.hortonworks.iotas.cache.redis.datastore.writer;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.redis.datastore.DataStore;

import java.util.Map;

public abstract class DataStoreWriter<K,V> {
    protected Cache<K,V> cache;
    protected DataStore<K, V> dataStore;

    public DataStoreWriter(Cache<K, V> cache, DataStore<K,V> dataStore) {
        this.cache = cache;
        this.dataStore = dataStore;
    }

    public void write(K key, V val) {
        dataStore.write(key, cache.get(key));
    }

    public abstract void writeAll(Map<K, V> entries);

}
