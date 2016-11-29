package org.apache.streamline.cache.config.builder;

import org.apache.streamline.cache.services.io.CacheLoader;
import org.apache.streamline.cache.services.io.CacheReader;
import org.apache.streamline.cache.services.io.CacheWriter;

public class CacheConfig<K,V> {
    private String cacheId;    // cannot be null

    private Class<K> key;      // cannot be null
    private Class<V> val;      // cannot be null

    private CacheType type;    // cannot be null

    private Eviction<?> eviction;    // can be null
    private Expiry  expiry;          // can be null

    private CacheLoader<K,V> loader;    // can be null
    private CacheReader<K,V> reader;    // can be null
    private CacheWriter<K,V> writer;    // can be null

    // Package protected such that builder is used to create this cache
    CacheConfig(String cacheId, Class<K> key, Class<V> val, CacheType type, Eviction<?> eviction, Expiry expiry,
                CacheLoader<K, V> loader, CacheReader<K, V> reader, CacheWriter<K, V> writer) {
        this.cacheId = cacheId;
        this.key = key;
        this.val = val;
        this.type = type;
        this.eviction = eviction;
        this.expiry = expiry;
        this.loader = loader;
        this.reader = reader;
        this.writer = writer;
    }

    public String getCacheId() {
        return cacheId;
    }

    public Class<K> getKey() {
        return key;
    }

    public Class<V> getVal() {
        return val;
    }

    public CacheType getType() {
        return type;
    }

    public Eviction<?> getEviction() {
        return eviction;
    }

    public Expiry getExpiry() {
        return expiry;
    }

    public CacheLoader<K, V> getLoader() {
        return loader;
    }

    public CacheReader<K, V> getReader() {
        return reader;
    }

    public CacheWriter<K, V> getWriter() {
        return writer;
    }

    public boolean isLoadable() {
        return loader != null;
    }

    public boolean isReadable() {
        return reader != null;
    }

    public boolean isWritable() {
        return writer != null;
    }

    @Override
    public String toString() {
        return "CacheConfig{" +
                "cacheId='" + cacheId + '\'' +
                ", key=" + key +
                ", val=" + val +
                ", type=" + type +
                ", eviction=" + eviction +
                ", expiry=" + expiry +
                ", loader=" + loader +
                ", reader=" + reader +
                ", writer=" + writer +
                '}';
    }
}
