package org.apache.streamline.cache.decorators;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.services.io.CacheWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WriteThroughCache<K,V> extends DelegateCache<K,V> {
    protected static final Logger LOG = LoggerFactory.getLogger(WriteThroughCache.class);

    private final CacheWriter<K, V> cacheWriter;

    public WriteThroughCache(Cache<K, V> delegate, CacheWriter<K, V> cacheWriter) {
        super(delegate);
        this.cacheWriter = cacheWriter;
    }

    @Override
    public void put(K key, V val) {
        if (cacheWriter != null) {
            cacheWriter.write(key, val);
        }
        delegate.put(key, val);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        if (cacheWriter != null) {
            cacheWriter.writeAll(entries);
        }
        delegate.putAll(entries);
    }

    @Override
    public void remove(K key) {
        if (cacheWriter != null) {
            cacheWriter.delete(key);
        }
        delegate.remove(key);
    }

    @Override
    public void removeAll(Collection<? extends K> keys) {
        if (cacheWriter != null) {
            cacheWriter.deleteAll(keys);
        }
        delegate.removeAll(keys);
    }
}
