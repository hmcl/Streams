package org.apache.streamline.cache.services.io;

import org.apache.streamline.cache.services.Service;

import java.util.Collection;
import java.util.Map;

/**
 *  Load entries (data) from the underlying data store.
 *  Methods that receive a collection of keys should be optimized for bulk operations
 *  @param <K>   Type of the key
 *  @param <V>   Type of the value
 **/
public interface CacheLoader<K, V> extends Service {
    /** Gets called when {@link CacheLoader} completes loading all keys */
    interface Listener<K,V> {
        void onCacheLoaded(Map<K, V> loaded);

        void onCacheLoadingFailure(Throwable t);
    }

    void loadAll(final Collection<? extends K> keys, Listener<K,V> listener);
}
