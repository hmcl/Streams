package com.hortonworks.iotas.cache.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.stats.CacheStats;
import com.hortonworks.iotas.cache.writer.WriterStrategy;
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorableKey;
import com.hortonworks.iotas.storage.StorageException;
import com.hortonworks.iotas.storage.StorageManager;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by hlouro on 8/6/15.
 */
public class GuavaCache<K extends StorableKey,V extends Storable> implements Cache<K,V> {
    private final StorageManager<V> dao;
    private final LoadingCache<K,V> guavaCache;
    private final WriterStrategy writer;


    public GuavaCache(final StorageManager<V> dao, CacheBuilder guavaCacheBuilder) {
        this(dao, guavaCacheBuilder, null);
    }

    public GuavaCache(final StorageManager<V> dao, CacheBuilder guavaCacheBuilder, WriterStrategy writerStrategy) {
        this.dao = dao;
        this.writer = writerStrategy;
        this.guavaCache = guavaCacheBuilder.build(new CacheLoader<K, V>() {
            @Override
            public V load(K key) throws StorageException {
                return dao.get(key);
            }
        });
    }

    public V get(K key) throws StorageException {
        try {
            return guavaCache.get(key);
        } catch (ExecutionException e) {
            throw new StorageException(e);
        }
    }

    public ImmutableMap<K, V> getAllPresent(Iterable<? extends K> keys) {
        return guavaCache.getAllPresent(keys);
    }

    public void put(K key, V value) {
        if (writer != null) {
            writer.add(value);
        }
        guavaCache.put(key, value);
    }


    public void putAll(Map<? extends K, ? extends V> map) {
        if (writer != null) {
            for (V value : map.values()) {
                writer.add(value);
            }
        }
        guavaCache.putAll(map);
    }

    public void remove(K key) {
        guavaCache.invalidate(key);
    }

    public ImmutableMap<K, V> removeAllPresent(Iterable<? extends K> keys) {
        final ImmutableMap<K, V> allPresent = guavaCache.getAllPresent(keys);
        guavaCache.invalidateAll(keys);
        return allPresent;
    }

    public void clear() {
        guavaCache.invalidateAll();
    }

    public long size() {
        return guavaCache.size();
    }

    //TODO
    public CacheStats stats() {
        return null;
    }

    public StorageManager<V> getDao() {
        return dao;
    }
}
