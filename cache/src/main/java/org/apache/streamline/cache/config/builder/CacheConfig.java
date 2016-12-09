package org.apache.streamline.cache.config.builder;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilderSpec;

import org.apache.streamline.cache.config.eviction.Eviction;
import org.apache.streamline.cache.config.expiry.Expiry;
import org.apache.streamline.cache.services.Service;
import org.apache.streamline.cache.services.io.CacheLoader;
import org.apache.streamline.cache.services.io.CacheReader;
import org.apache.streamline.cache.services.io.CacheWriter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class CacheConfig<K, V> {
    private Class<K> key;      // cannot be null    // TODO: Do I need this ?
    private Class<V> val;      // cannot be null

    private CacheType type;    // cannot be null

    private CacheLoader<K, V> loader;    // can be null
    private CacheReader<K, V> reader;    // can be null
    private CacheWriter<K, V> writer;    // can be null

    private Supplier<Collection<? extends Service>> services;   // memoize services

    // Package protected such that builder is used to create this cache
    CacheConfig(Class<K> key, Class<V> val, CacheType type, CacheLoader<K, V> loader,
                CacheReader<K, V> reader, CacheWriter<K, V> writer) {
        this.key = key;
        this.val = val;
        this.type = type;
        this.loader = loader;
        this.reader = reader;
        this.writer = writer;
        this.services = Suppliers.memoize(this::getActiveServices);
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

    public Optional<CacheLoader<K, V>> getLoader() {
        return Optional.ofNullable(loader);
    }

    public Optional<CacheReader<K, V>> getReader() {
        return Optional.ofNullable(reader);
    }

    public Optional<CacheWriter<K, V>> getWriter() {
        return Optional.ofNullable(writer);
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

    public Optional<Collection<? extends Service>> getServices() {
        return services.get().isEmpty() ? Optional.empty() : Optional.of(services.get());
    }

    private Collection<? extends Service> getActiveServices() {
        final List<Service> servs = new ArrayList<>(3);

        if (isReadable()) {
            servs.add(reader);
        }

        if (isLoadable()) {
            servs.add(loader);
        }

        if (isWritable()) {
            servs.add(writer);
        }
        return servs;
    }

    /**
     * To be implemented by subclasses
     */
    public <C> Optional<C> getDelegateCacheConfig() {
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "CacheConfig{" +
                ", key=" + key +
                ", val=" + val +
                ", type=" + type +
                ", loader=" + loader +
                ", reader=" + reader +
                ", writer=" + writer +
                '}';
    }

    public static class Guava<K, V> extends CacheConfig<K, V> {

        Guava(Class<K> key, Class<V> val, CacheType type, Eviction eviction, Expiry expiry, CacheLoader<K, V> loader, CacheReader<K, V> reader, CacheWriter<K, V> writer) {
            super(key, val, type, eviction, expiry, loader, reader, writer);
        }

        @Override
        @SuppressWarnings("unchecked")
        public CacheBuilderSpec getDelegateCacheConfig() {
            return CacheBuilderSpec.parse("");
        }
    }
}
