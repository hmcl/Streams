package com.hortonworks.iotas.cache.redis;

import java.util.Collection;
import java.util.Map;

public interface DataStoreReaderWriter<K, V> extends DataStoreReader<K,V> {
    void write(K key, V val);

    void writeAll(Map<K, V> entries);

    void delete(K key);

    void deleteAll(Collection<K> keys);
}
