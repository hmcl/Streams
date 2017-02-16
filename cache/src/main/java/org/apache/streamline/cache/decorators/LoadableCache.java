package org.apache.streamline.cache.decorators;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.services.io.CacheLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Objects;

public class LoadableCache<K, V> extends DelegateCache<K, V> {
    protected static final Logger LOG = LoggerFactory.getLogger(LoadableCache.class);

    private final CacheLoader<K, V> cacheLoader;

    public LoadableCache(Cache<K, V> delegate, CacheLoader<K, V> cacheLoader) {
        super(delegate);
        Objects.requireNonNull(cacheLoader, "CacheLoader implementation required. If Cache not loadable, use DelegateCache.");
        this.cacheLoader = cacheLoader;
    }

    public void loadAll(Collection<? extends K> keys, CacheLoader.Listener<K, V> listener) {
        cacheLoader.loadAll(keys, listener);
        LOG.debug("Entries associated with keys [{}] successfully loaded into cache", keys);
    }
}
