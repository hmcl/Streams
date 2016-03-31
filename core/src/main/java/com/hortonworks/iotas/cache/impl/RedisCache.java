package com.hortonworks.iotas.cache.impl;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.stats.CacheStats;
import com.hortonworks.iotas.storage.exception.StorageException;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class RedisCache<K,V> implements Cache<K,V> {

    private RedisConnection<K,V> redisConnection;


    @Override
    public V get(K key) throws StorageException {
        return redisConnection.hget(key);
    }

    @Override
    public ImmutableMap<K, V> getAllPresent(Iterable<? extends K> keys) {
        return null;
    }

    @Override
    public void put(K key, V value) {
        redisConnection.set(key, value);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {

    }

    @Override
    public void remove(K key) {

    }

    @Override
    public ImmutableMap<K, V> removeAllPresent(Iterable<? extends K> keys) {
        return null;
    }

    @Override
    public void clear() {
        Collection<? extends K> keys = new ArrayList<>();
        removeAllPresent(keys);
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public CacheStats stats() {
        return null;
    }
}
