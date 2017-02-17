package org.apache.streamline.cache.manager;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.config.builder.CacheConfig;
import org.apache.streamline.cache.services.CacheService;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

public class LocalCacheRuntimeInfo<K,V,C> implements CacheRuntimeInfo<K,V,C> {
    private String cacheId;
    private Cache<K, V> cache;
    private CacheConfig<K,V,C> cacheConfig;

    public LocalCacheRuntimeInfo(String cacheId, Cache<K, V> cache) {
        this(cacheId, cache, null);
    }

    public LocalCacheRuntimeInfo(String cacheId, Cache<K, V> cache, CacheConfig<K,V,C> cacheConfig) {
        Objects.requireNonNull(cacheId);
        Objects.requireNonNull(cache);
        this.cacheId = cacheId;
        this.cache = cache;
        this.cacheConfig = cacheConfig;
    }

    @Override
    public String getCacheId() {
        return cacheId;
    }

    @Override
    public Cache<K, V> getCache() {
        return cache;
    }

    @Override
    public Optional<CacheConfig<K,V,C>> getCacheConfig() {
        return Optional.ofNullable(cacheConfig);
    }

    @Override
    public Optional<Collection<? extends CacheService>> getCacheServices() {
        if (getCacheConfig().isPresent()) {
            return cacheConfig.getCacheServices();
        }
        return Optional.empty();
    }

    /**
     * @return true if two objects have the same cache id, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return cacheId.equals(((LocalCacheRuntimeInfo<?,?,?>) o).cacheId);
    }

    @Override
    public int hashCode() {
        return cacheId.hashCode();
    }
}
