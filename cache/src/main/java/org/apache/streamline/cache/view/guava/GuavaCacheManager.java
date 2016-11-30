package org.apache.streamline.cache.view.guava;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheBuilderSpec;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.Weigher;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.CacheManager;
import org.apache.streamline.cache.config.builder.CacheConfig;
import org.apache.streamline.cache.config.eviction.Eviction;
import org.apache.streamline.cache.config.expiry.Expiry;
import org.apache.streamline.cache.decorators.LoadableCache;
import org.apache.streamline.cache.decorators.WriteThroughCache;
import org.apache.streamline.cache.exception.CacheAlreadyExistsException;
import org.apache.streamline.cache.exception.CacheException;
import org.apache.streamline.cache.exception.InvalidCacheConfigException;
import org.apache.streamline.cache.services.Service;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class GuavaCacheManager implements CacheManager {
    private ConcurrentMap<String, Cache<?, ?>> caches = new ConcurrentHashMap<>();

    @Override @SuppressWarnings("unchecked")
    public <K, V> GuavaCache<K,V> createCache(String cacheId, CacheConfig<K, V> config) {
        return addCache(cacheId, createFromConfig(config));
    }

    private <K, V, T extends Cache<K, V>> T createFromConfig(final CacheConfig<K, V> config) {

        @SuppressWarnings("unchecked")
        final CacheBuilder<K, V> builder = (CacheBuilder<K, V>) CacheBuilder.from((CacheBuilderSpec) config.getDelegateCacheConfig());

        com.google.common.cache.Cache<K, V> guavaCache;
        if (config.isReadable()) {

//            final LoadingCache<K, V> loadingCache = builder.build(new CacheLoader<K, V>() {
            guavaCache = builder.build(new CacheLoader<K, V>() {
                @Override
                public V load(K key) throws Exception {
                    return config.getReader().read(key);
                }
            });
        } else {
             guavaCache = builder.build();
        }

        T cacheView = new GuavaCache<K,V>(guavaCache);

        if (config.isLoadable()) {
            cacheView = new LoadableCache<>(cacheView, config.getLoader());
        }

        if (config.isWritable()) {
            cacheView = new WriteThroughCache<>(cacheView, config.getWriter());
        }

        return cacheView;
    }

    @Override
    public <K, V, T extends Cache<K, V>> T addCache(String cacheId, T cache) {
        if (caches.putIfAbsent(cacheId, cache) != null) {
            throw new CacheAlreadyExistsException("Cache with id [" + cacheId + "] already exists");
        }
        return cache;
    }

    @Override @SuppressWarnings("unchecked")
    public <K, V, T extends Cache<K, V>> T getCache(String cacheId) {
        return (T) caches.get(cacheId);
    }

    @Override
    public void removeCache(String cacheId) {
        caches.remove(cacheId);
    }

    @Override
    public void init() throws CacheException {

    }

    @Override
    public void close() throws CacheException {

    }

    @Override
    public Collection<? extends Service> getServices() {
        return null;
    }

    @Override
    public Collection<? extends CacheConfig> getConfigs() {
        return null;
    }
}
