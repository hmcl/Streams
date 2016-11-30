package org.apache.streamline.cache.manager;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.config.builder.CacheConfig;
import org.apache.streamline.cache.exception.CacheException;
import org.apache.streamline.cache.services.Service;

import java.util.Collection;
import java.util.Map;

public interface CacheManager extends AutoCloseable {

    <K,V> Cache<K, V> createCache(String cacheId, CacheConfig<K, V> config);

    <K,V> Cache<K, V> addCache(String cacheId, Cache<K, V> cache);

    <K,V> Cache<K, V> getCache(String cacheId);

    /**
     * closes and removes the cache with the given id from this manager
     */
    <K,V> Cache<K, V> removeCache(String cacheId);

    /** Configs associated with each cache. Not all caches managed by this manager may have an associated config */
    Map<String, CacheConfig> getCacheConfigs();

    Map<String, Collection<Service>> getCacheServices();

    void init() throws CacheException;      // TODO Exception handling

    @Override
    void close() throws CacheException;
}
