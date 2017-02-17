package org.apache.streamline.cache.manager;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.config.builder.CacheConfig;
import org.apache.streamline.cache.services.CacheService;

import java.util.Collection;
import java.util.Optional;

/**
 * Wrapper for Cache, Service, Cache Config
 *
 * @param <K> Type of the key
 * @param <V> Type of the value
 * @param <C> Type of the underlying cache configuration
 */
public interface CacheRuntimeInfo<K,V,C> {

    String getCacheId();

    Cache<K,V> getCache();

    /**
     * {@link CacheConfig} info used to create the {@link Cache}
     */
    Optional<CacheConfig<K,V,C>> getCacheConfig();

    /**
     * {@link CacheService}s used by this {@link Cache}
     */
    Optional<Collection<? extends CacheService>> getCacheServices();
}
