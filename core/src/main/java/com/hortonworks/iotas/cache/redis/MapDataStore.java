package com.hortonworks.iotas.cache.redis;

import java.util.Collection;
import java.util.Map;

public interface MapDataStore<K, V> {
    V read(K key);

    Map<K, V> readAll(Collection<K> keys);

    void write(K key, V val);

    void writeAll(Map<K, V> entries);

    void delete(K key);

    void deleteAll(Collection<K> keys);
}
