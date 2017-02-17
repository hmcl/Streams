package org.apache.streamline.cache.manager;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.ManagedCache;
import org.apache.streamline.cache.config.builder.CacheConfig;
import org.apache.streamline.cache.exception.CacheException;

import java.util.Map;

public interface CacheManager extends AutoCloseable {

    /**
     * Initiates all {@link ManagedCache} instances managed by this {@link CacheManager}.
     * No-op for non-{@link ManagedCache} {@link Cache} instances
     **/
    void init() throws CacheException;      // TODO Exception handling

    /**
     * Creates a {@link Cache} instance using the {@link CacheConfig} specified and associates it with the id specified
     */
    <K,V,C> Cache<K,V> createCache(String cacheId, CacheConfig<K,V,C> config);

    /**
     * Adds the {@link Cache} instance specified and associates it with the id specified
     */
    <K,V> Cache<K,V> addCache(String cacheId, Cache<K,V> cache);

    /**
     * @return the {@link Cache} instance associated with the id specified
     */
    <K,V> Cache<K,V> getCache(String cacheId);

    /**
     * Closes and removes the {@link Cache} with the given id from this {@link CacheManager}.
     * No-op if no cache with given id exists.
     */
    <K,V> Cache<K,V> removeCache(String cacheId);

    /**
     * @return {@link CacheRuntimeInfo} for
     */
    <K,V,C> Map<String, CacheRuntimeInfo<K,V,C>> getRuntimeInfo();

    /**
     * Closes all {@link ManagedCache} instances managed by this {@link CacheManager}.
     * No-op for non-{@link ManagedCache} {@link Cache} instances
     **/
    @Override
    void close() throws CacheException;
}
