package org.apache.streamline.cache.view.guava;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.Weigher;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.CacheManager;
import org.apache.streamline.cache.config.builder.CacheConfig;
import org.apache.streamline.cache.config.eviction.Eviction;
import org.apache.streamline.cache.config.expiry.Expiry;
import org.apache.streamline.cache.exception.CacheAlreadyExistsException;
import org.apache.streamline.cache.exception.CacheException;
import org.apache.streamline.cache.exception.InvalidCacheConfigException;
import org.apache.streamline.cache.services.Service;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class GuavaCacheManager implements CacheManager {
    private ConcurrentMap<String, Cache<?, ?>> caches = new ConcurrentHashMap<>();

    @Override @SuppressWarnings("unchecked")
    public <K, V, T extends Cache<K, V>> T createCache(String cacheId, CacheConfig<K, V> config) {
        return addCache(cacheId, (T)createFromConfig(config));
    }

    private <K, V, T extends Cache<K, V>> T createFromConfig(final CacheConfig<K, V> config) {

        @SuppressWarnings("unchecked")
        final CacheBuilder<K, V> builder = (CacheBuilder<K, V>) CacheBuilder.newBuilder();

        // config Expiry
        Expiry expiry = config.getExpiry();

        if (expiry.isOnCreation() && expiry.isOnUpdate()) {
            if (!expiry.onUpdate().equals(expiry.onCreation())) {
                throw new InvalidCacheConfigException("Update and creation expiry must have the same value");
            }
        }

        if (expiry.isOnCreation()) {
            builder.expireAfterWrite(expiry.onCreation().getDuration(), expiry.onCreation().getTimeUnit());
        } else if (expiry.isOnUpdate()) {
            builder.expireAfterWrite(expiry.onUpdate().getDuration(), expiry.onUpdate().getTimeUnit());
        }

        if (expiry.isOnAccess()) {
            builder.expireAfterAccess(expiry.onAccess().getDuration(), expiry.onAccess().getTimeUnit());
        }

        // config Eviction
        final Eviction eviction = config.getEviction();

        if (eviction.isEntries()) {
            builder.maximumSize(eviction.entries());
        }

        if (eviction.isSize()) {
            builder.weigher((Weigher<K, V>) (key, value) -> {


            });
            builder.maximumWeight(size/weigh)


        }

//        if (config.)

        com.google.common.cache.Cache<K, V> guavaCache;
        if (config.isReadable()) {

//            final LoadingCache<K, V> loadingCache = builder.build(new CacheLoader<K, V>() {
            guavaCache = builder.build(new CacheLoader<K, V>() {
                @Override
                public V load(K key) throws Exception {
                    return config.getReader().read(key);
                }
            });
        } else {
             guavaCache = builder.build();
        }

        Cache<K,V> cache = new GuavaCache<>(guavaCache)






    }

    @Override
    public <K, V, T extends Cache<K, V>> T addCache(String cacheId, T cache) {
        if (caches.putIfAbsent(cacheId, cache) != null) {
            throw new CacheAlreadyExistsException("Cache with id [" + cacheId + "] already exists");
        }
        return cache;
    }

    @Override @SuppressWarnings("unchecked")
    public <K, V, T extends Cache<K, V>> T getCache(String cacheId) {
        return (T) caches.get(cacheId);
    }

    @Override
    public void removeCache(String cacheId) {
        caches.remove(cacheId);
    }

    @Override
    public void init() throws CacheException {

    }

    @Override
    public void close() throws CacheException {

    }

    @Override
    public Collection<? extends Service> getServices() {
        return null;
    }

    @Override
    public Collection<? extends CacheConfig> getConfigs() {
        return null;
    }
}
