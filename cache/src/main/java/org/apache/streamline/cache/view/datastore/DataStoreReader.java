package com.hortonworks.iotas.cache.view.datastore;

import java.util.Collection;
import java.util.Map;

/** Read entries (data) from the underlying data store.
 *  Methods that receive a collection of keys should be optimized for bulk operations
 *  @param <K>   Type of the key
 *  @param <V>   Type of the value
 **/
public interface DataStoreReader<K, V> {
    /** Read one key */
    V read(K key);

    /** Read a collection of keys */
    Map<K, V> readAll(Collection<? extends K> keys);
}
