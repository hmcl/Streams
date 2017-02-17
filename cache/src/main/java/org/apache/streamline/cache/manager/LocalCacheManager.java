package org.apache.streamline.cache.manager;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.ManagedCache;
import org.apache.streamline.cache.config.builder.CacheConfig;
import org.apache.streamline.cache.exception.CacheAlreadyExistsException;
import org.apache.streamline.cache.exception.CacheException;
import org.apache.streamline.cache.exception.CacheNotFoundException;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

@SuppressWarnings("unchecked")
public abstract class LocalCacheManager implements CacheManager {
    protected ConcurrentMap<String, CacheRuntimeInfo<?,?,?>> caches = new ConcurrentHashMap<>();

    @Override
    public abstract <K, V, C> Cache<K, V> createCache(String cacheId, CacheConfig<K, V, C> config);

    @Override
    public <K, V> Cache<K, V> addCache(String cacheId, Cache<K, V> cache) {
        if (caches.putIfAbsent(cacheId, new LocalCacheRuntimeInfo<>(cacheId, cache)) != null) {
            throw new CacheAlreadyExistsException("Cache with id [" + cacheId + "] already exists");
        }
        return cache;
    }

    protected <K, V, C> Cache<K, V> addCache(String cacheId, Cache<K, V> cache, CacheConfig<K, V, C> config) {
        if (caches.putIfAbsent(cacheId, new LocalCacheRuntimeInfo<>(cacheId, cache, config)) != null) {
            throw new CacheAlreadyExistsException("Cache with id [" + cacheId + "] already exists");
        }
        return cache;
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheId) {
        if (caches.containsKey(cacheId)) {
            return (Cache<K, V>) caches.get(cacheId);
        }
        throw new CacheNotFoundException(String.format("Cache with id [%s] not found", cacheId));
    }

    @Override
    public <K, V> Cache<K, V> removeCache(String cacheId) {
        return (Cache<K, V>) caches.remove(cacheId);
    }

    @Override
    public Map<String, CacheRuntimeInfo<?,?,?>> getRuntimeInfo() {
        return Collections.unmodifiableMap(caches);
    }

    @Override
    public void init() throws CacheException {
        exec(ManagedCache::init);
    }

    @Override
    public void close() throws CacheException {
        exec(ManagedCache::close);
    }

    private void exec(Consumer<ManagedCache> method) {
        caches.values().stream().filter((cache) -> cache instanceof ManagedCache)
                .forEach((cache) -> method.accept((ManagedCache) cache));
    }
}
