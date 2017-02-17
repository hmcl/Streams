package org.apache.streamline.cache.config.builder;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilderSpec;

import org.apache.streamline.cache.config.eviction.Eviction;
import org.apache.streamline.cache.config.expiry.Expiry;
import org.apache.streamline.cache.services.CacheService;
import org.apache.streamline.cache.services.io.CacheLoader;
import org.apache.streamline.cache.services.io.CacheReader;
import org.apache.streamline.cache.services.io.CacheWriter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * 
 * @param <K> Type of the key
 * @param <V> Type of the value
 * @param <C> Type of the underlying cache configuration
 */
public class CacheConfig<K, V, C> {
    private final Class<K> key;      // cannot be null    // TODO: Do I need this ?
    private final Class<V> val;      // cannot be null

    private final CacheType type;    // cannot be null

    private final C cacheConfig;
    private final CacheLoader<K, V> loader;    // can be null
    private final CacheReader<K, V> reader;    // can be null
    private final CacheWriter<K, V> writer;    // can be null

    private final Supplier<Collection<? extends CacheService>> cacheServices;   // memoize services

    public CacheConfig(Class<K> key, Class<V> val, CacheType type, C cacheConfig, CacheLoader<K, V> loader,
                CacheReader<K, V> reader, CacheWriter<K, V> writer) {
        this.key = key;
        this.val = val;
        this.type = type;
        this.cacheConfig = cacheConfig;
        this.loader = loader;
        this.reader = reader;
        this.writer = writer;
        this.cacheServices = Suppliers.memoize(this::getActiveServices);
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

    public Optional<Collection<? extends CacheService>> getCacheServices() {
        return cacheServices.get().isEmpty() ? Optional.empty() : Optional.of(cacheServices.get());
    }

    private Collection<? extends CacheService> getActiveServices() {
        final List<CacheService> services = new ArrayList<>(3);

        if (isReadable()) {
            services.add(reader);
        }

        if (isLoadable()) {
            services.add(loader);
        }

        if (isWritable()) {
            services.add(writer);
        }
        return services;
    }

    public Optional<C> getDelegateCacheConfig() {
        return Optional.ofNullable(cacheConfig);
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
