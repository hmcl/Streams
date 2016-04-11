package com.hortonworks.iotas.cache.redis.loader;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.redis.datastore.DataStore;

import java.util.Collection;

public class CacheLoaderSync<K,V> extends CacheLoader<K,V> {
    public CacheLoaderSync(Cache<K, V> cache, DataStore<K,V> dataStore) {
        super(cache, dataStore);
    }

    public void loadAll(Collection<? extends K> keys) {
        cache.putAll(dataStore.readAll(keys));
    }
}
