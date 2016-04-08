package com.hortonworks.iotas.cache.redis;

import java.util.Collection;
import java.util.Map;

public interface DataStoreReader<K, V> {
    V read(K key);

    Map<K, V> readAll(Collection<K> keys);
}
