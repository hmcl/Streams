package com.hortonworks.iotas.cache.redis.service;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.redis.datastore.DataStore;
import com.hortonworks.iotas.cache.redis.datastore.DataStoreReader;
import com.hortonworks.iotas.cache.redis.loader.CacheLoader;

import java.util.Collection;

public class CacheService<K,V> {
    private final Cache<K,V> cache;
    private DataStore<K,V> dataStore;
    private CacheLoader<K,V> cacheLoader;

    public CacheService(CacheServiceFactory<K,V> factory) {
        this.cache = factory.createCache();
        this.dataStore = factory.createDataStore();
    }

    public Cache<K,V> getCache() {
        return cache;
    }

    public DataStore<K,V> getDataStore() {
        return dataStore;
    }

    public void load(K key) {
        cacheLoader.load(key);
    }

    public void loadAll(Collection<? extends K> keys) {

    }


}
