package com.hortonworks.iotas.cache.view.datastore;

import java.util.Collection;
import java.util.Map;

/** Implementations contain the logic to read entries (data) from the underlying data store */
public interface DataStoreReader<K, V> {
    /** Read one key */
    V read(K key);

    /** Read a collection of keys. This method should be optimized for bulk operations */
    Map<K, V> readAll(Collection<? extends K> keys);
}
