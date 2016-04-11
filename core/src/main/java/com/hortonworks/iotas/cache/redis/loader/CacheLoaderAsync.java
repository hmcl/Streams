package com.hortonworks.iotas.cache.redis.loader;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.redis.datastore.DataStore;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CacheLoaderAsync<K,V> extends CacheLoader<K,V> {
    private static final int DEFAULT_NUM_THREADS = 5;
    private final ExecutorService executorService;

    public CacheLoaderAsync(Cache<K, V> cache, DataStore<K,V> dataStore) {
        this(cache, dataStore, Executors.newFixedThreadPool(DEFAULT_NUM_THREADS));
    }

    public CacheLoaderAsync(Cache<K, V> cache, DataStore<K,V> dataStore, ExecutorService executorService) {
        super(cache, dataStore);
        this.executorService = executorService;
    }

    public void loadAll(final Collection<? extends K> keys) {
        executorService.submit(new Callable<Map<K, V>>() {
            @Override
            public Map<K, V> call() throws Exception {
                cache.putAll(dataStore.readAll(keys));
                return null;
            }
        });
    }
}
