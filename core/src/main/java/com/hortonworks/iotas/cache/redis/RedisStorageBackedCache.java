package com.hortonworks.iotas.cache.redis;

import com.hortonworks.iotas.cache.redis.service.CacheService;

import java.util.Map;

public class RedisStorageBackedCache<K,V> extends RedisStringsCache<K, V> {
    private CacheService cacheService;

    public RedisStorageBackedCache(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    @Override
    public void put(K key, V value) {
        super.put(key, value);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        super.putAll(m);
    }

    @Override
    public void remove(K key) {
        super.remove(key);
    }

    @Override
    public Map<K, V> removeAllPresent(Iterable<? extends K> keys) {
        return super.removeAllPresent(keys);
    }

    @Override
    public void clear() {
        super.clear();
    }
}
