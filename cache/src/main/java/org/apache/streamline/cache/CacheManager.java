package org.apache.streamline.cache;

import org.apache.streamline.cache.config.builder.CacheConfig;
import org.apache.streamline.cache.exception.CacheException;
import org.apache.streamline.cache.services.Service;

import java.util.Collection;
import java.util.Map;

public interface CacheManager extends AutoCloseable {

    <K,V,T extends Cache<K, V>> T createCache(String cacheId, CacheConfig<K, V> config);

    <K,V,T extends Cache<K, V>> T addCache(String cacheId, T cache);

    <K,V,T extends Cache<K, V>> T getCache(String cacheId);

    /**
     * closes and removes cache from this manager
     */
    void removeCache(String cacheId);

    void init() throws CacheException;

    @Override
    void close() throws CacheException;

    Collection<? extends Service> getServices();

    Collection<? extends CacheConfig> getConfigs();

    /** Configs associated with each cache. Not all caches managed by this manager may have an associated config */
    Map<String, ? extends CacheConfig> getCacheConfigs();
}
