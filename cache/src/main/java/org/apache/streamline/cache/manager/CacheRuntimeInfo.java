package org.apache.streamline.cache.manager;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.config.builder.CacheConfig;
import org.apache.streamline.cache.services.CacheService;

import java.util.Collection;
import java.util.Optional;

/**
 * Wrapper for Cache, Service, Cache Config
 */
public interface CacheRuntimeInfo<K,V> {

    String getCacheId();

    Cache<K,V> getCache();

    /**
     * {@link CacheConfig} info used to create the {@link Cache}
     */
    Optional<CacheConfig> getCacheConfig();

    /**
     * {@link CacheService}s used by this {@link Cache}
     */
    Optional<Collection<? extends CacheService>> getCacheServices();
}
