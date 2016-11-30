package org.apache.streamline.cache.decorators;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.services.io.CacheReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ReadThroughCache<K,V> extends DelegateCache<K,V> {
    protected static final Logger LOG = LoggerFactory.getLogger(ReadThroughCache.class);

    private final CacheReader<K, V> cacheReader;

    public ReadThroughCache(Cache<K, V> delegate, CacheReader<K, V> cacheReader) {
        super(delegate);
        Objects.requireNonNull(cacheReader);
        this.cacheReader = cacheReader;
    }

    @Override
    public V get(K key) {
        V val = delegate.get(key);
        if (val == null && cacheReader != null) {     // cache miss
            val = cacheReader.read(key);              // in sync read through
            delegate.put(key, val);                  // load cache with value read from storage
        }
        return val;
    }

    @Override
    public Map<K, V> getAll(Collection<? extends K> keys) {  // TODO what if trying to load more keys than max number of keys that be kept in the cache ?
        Map<K, V> present = delegate.getAll(keys);
        LOG.debug("Entries existing in cache [{}].", present);

        if (cacheReader != null) {
            if (present == null || present.isEmpty()) {
                present = cacheReader.readAll(keys);                  // in sync read through
                delegate.putAll(present);                            // load cache with values read from storage
            } else if (present.size() < keys.size()) {                // not all values are in cache, i.e., some cache misses
                final Set<K> notPresent = new HashSet<>(keys);
                notPresent.removeAll(present.keySet());
                final Map<K, V> loaded = cacheReader.readAll(notPresent);   // in sync read through
                present.putAll(loaded);
                delegate.putAll(loaded);                                           // load cache with values read from storage
                LOG.debug("Keys non existing in cache: [{}]. Entries read from database and loaded into cache [{}]", notPresent, loaded);
            }
            present = delegate.getAll(keys);
        }
        LOG.debug("Entries existing in cache after loading [{}]", delegate.getAll(keys));
        return present;
    }
}
