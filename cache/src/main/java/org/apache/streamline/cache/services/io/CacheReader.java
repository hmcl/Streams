package org.apache.streamline.cache.services.io;

import org.apache.streamline.cache.services.CacheService;

import java.util.Collection;
import java.util.Map;

/**
 *  Read/Load entries (data) from the underlying data store.
 *  Methods that receive a collection of keys should be optimized for bulk operations
 *  @param <K>   Type of the key
 *  @param <V>   Type of the value
 **/
public interface CacheReader<K, V> extends CacheService {
    /** Load one key */
    V read(K key);

    /** Load a collection of keys */
    Map<K, V> readAll(Collection<? extends K> keys);
}
