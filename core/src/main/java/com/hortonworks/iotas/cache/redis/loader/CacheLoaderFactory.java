package com.hortonworks.iotas.cache.redis.loader;

import com.hortonworks.iotas.cache.redis.datastore.DataStore;

public interface CacheLoaderFactory<K,V> {
    public CacheLoader<K,V> createCacheLoader();
}
