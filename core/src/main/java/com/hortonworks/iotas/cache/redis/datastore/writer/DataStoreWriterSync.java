package com.hortonworks.iotas.cache.redis.datastore.writer;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.redis.datastore.DataStore;

import java.util.Collection;
import java.util.Map;

public abstract class DataStoreWriterSync<K,V> implements DataStoreWriter<K,V> {
    protected Cache<K,V> cache;
    protected DataStore<K, V> dataStore;

    public DataStoreWriterSync(Cache<K, V> cache, DataStore<K,V> dataStore) {
        this.cache = cache;
        this.dataStore = dataStore;
    }

    public void write(K key, V val) {   //TODO val missing
        dataStore.write(key, cache.get(key));
    }

    public void writeAll(Map<? extends K, ? extends V> entries) {
        dataStore.writeAll(entries);
    }

    public void delete(K key) {
        dataStore.delete(key);
    }

    public void deleteAll(Collection<? extends K> keys){
        dataStore.deleteAll(keys);
    }
}
