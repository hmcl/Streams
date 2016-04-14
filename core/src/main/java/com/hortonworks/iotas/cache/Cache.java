package com.hortonworks.iotas.cache;


import com.hortonworks.iotas.cache.stats.CacheStats;
import com.hortonworks.iotas.storage.exception.StorageException;

import java.util.Collection;
import java.util.Map;

/**
 * Created by hlouro on 8/6/15.
 */
public interface Cache<K, V> {
    V get(K key) throws StorageException;

    Map<K, V> getAllPresent(Collection<? extends K> keys);

    void put(K key, V val);

    void putAll(Map<? extends K,? extends V> entries);

    void remove(K key);

    Map<K, V> removeAllPresent(Iterable<? extends K> keys);

    void clear();

    long size();

    CacheStats stats();
}
