package com.hortonworks.iotas.cache.redis.loader;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.redis.datastore.DataStore;

import java.util.Collection;

public abstract class CacheLoader<K,V> {
    protected Cache<K,V> cache;
    protected DataStore<K, V> dataStore;

    public CacheLoader(Cache<K, V> cache, DataStore<K,V> dataStore) {
        this.cache = cache;
        this.dataStore = dataStore;
    }

    public void load(K key) {
        cache.put(key, dataStore.read(key));
    }

    public abstract void loadAll(Collection<? extends K> keys);
}
