package org.apache.streamline.cache;

import org.apache.streamline.cache.exception.CacheException;
import org.apache.streamline.cache.services.Service;

import java.util.Collection;

public interface CacheManager<K,V> extends AutoCloseable {

    Cache<K, V> createCache(String cacheId, CacheConfiguration<K, V> config);

    <K,V> Cache<K, V> createCache(String cacheId, CacheBuilderComplex<K,V> cacheBuilder);

    public Cache<K, V> createAndRegisterCache(String cacheId, CacheBuilderComplex<K,V> cacheBuilder);

    public Cache<K, V> createAndRegisterCache(String cacheId, CacheConfig cacheConfig);

    public void addCache(String cacheId, Cache<K,V> cache);

    public Cache<K, V> getCache(String cacheId);

    /**
     * closes and removes cache from this manager
     */
    public void removeCache(String cacheId);

    public void init() throws CacheException;

    @Override
    public void close() throws CacheException;

    public Collection<? extends Service> getServices();
}
