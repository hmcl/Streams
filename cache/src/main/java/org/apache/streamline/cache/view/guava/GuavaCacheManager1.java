package org.apache.streamline.cache.view.guava;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.manager.CacheManager;
import org.apache.streamline.cache.config.builder.CacheConfig;
import org.apache.streamline.cache.exception.CacheException;
import org.apache.streamline.cache.services.Service;

import java.util.Map;

public class GuavaCacheManager1 implements CacheManager {
    @Override
    public <K, V> Cache<K, V> createCache(String cacheId, CacheConfig<K, V> config) {
        return null;
    }

    @Override
    public <K, V> Cache<K, V> addCache(String cacheId, Cache<K, V> cache) {
        return null;
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheId) {
        return null;
    }

    @Override
    public <K, V> Cache<K, V> removeCache(String cacheId) {
        return null;
    }

    @Override
    public void init() throws CacheException {

    }

    @Override
    public void close() throws CacheException {

    }

    @Override
    public Map<String, Service> getCacheServices() {
        return null;
    }

    @Override
    public Map<String, CacheConfig> getCacheConfigs() {
        return null;
    }
}
