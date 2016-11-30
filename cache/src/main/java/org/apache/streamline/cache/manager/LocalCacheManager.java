package org.apache.streamline.cache.manager;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.ManagedCache;
import org.apache.streamline.cache.config.builder.CacheConfig;
import org.apache.streamline.cache.exception.CacheAlreadyExistsException;
import org.apache.streamline.cache.exception.CacheException;
import org.apache.streamline.cache.exception.CacheNotFoundException;
import org.apache.streamline.cache.services.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@SuppressWarnings("unchecked")
public abstract class LocalCacheManager implements CacheManager {
    protected ConcurrentMap<String, Cache<?, ?>> caches = new ConcurrentHashMap<>();
    protected ConcurrentMap<String, CacheConfig<?, ?>> configs = new ConcurrentHashMap<>();
    protected ConcurrentMap<String, Collection<Service>> services = new ConcurrentHashMap<>();

    @Override
    public abstract <K, V> Cache<K, V> createCache(String cacheId, CacheConfig<K, V> config);

    @Override
    public <K, V> Cache<K, V> addCache(String cacheId, Cache<K, V> cache) {
        if (caches.putIfAbsent(cacheId, cache) != null) {
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
    public Map<String, CacheConfig> getCacheConfigs() {
        return Collections.unmodifiableMap(configs);
    }

    @Override
    public Map<String, Collection<Service>> getCacheServices() {
        return Collections.unmodifiableMap(services);
    }

    @Override
    public void init() throws CacheException {
        for (Cache<?, ?> cache : caches.values()) {
            if (cache instanceof ManagedCache) {
                ((ManagedCache) cache).init();
            }
        }
    }

    @Override
    public void close() throws CacheException {
        for (Cache<?, ?> cache : caches.values()) {
            if (cache instanceof ManagedCache) {
                ((ManagedCache) cache).close();
            }
        }
    }

    // ======== Bookkeeping methods for use in subclasses ====

    protected <K, V> void addConfig(String cacheId, CacheConfig<K, V> config) {
        if (configs.putIfAbsent(cacheId, config) != null) {
            throw new CacheException("Cache with id [" + cacheId + "] already configured");
        }
    }

    protected <K, V> void addServices(String cacheId, CacheConfig<K, V> config) {
        services.putIfAbsent(cacheId, new ArrayList<>());
        if (config.isReadable()) {
            services.get(cacheId).add(config.getReader());
        }
        if (config.isLoadable()) {
            services.get(cacheId).add(config.getLoader());
        }
        if (config.isWritable()) {
            services.get(cacheId).add(config.getWriter());
        }
    }

    // used to revert state in case of exception
    protected void removeServices(String cacheId) {
        services.remove(cacheId);
    }

    protected <K, V> void removeConfig(String cacheId) {
        configs.remove(cacheId);
    }
}
