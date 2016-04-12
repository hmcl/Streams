package com.hortonworks.iotas.cache.redis.datastore;

import java.util.Collection;
import java.util.Map;

public interface DataStore<K, V> {
    V read(K key);

    Map<K, V> readAll(Collection<? extends K> keys);

    void write(K key, V val);

    void writeAll(Map<? extends K, ? extends V> entries);

    void delete(K key);

    void deleteAll(Collection<? extends K> keys);
}
