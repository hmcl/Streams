package org.apache.streamline.cache;

import org.apache.streamline.cache.exception.CacheAlreadyExistsException;
import org.apache.streamline.cache.exception.CacheNotFoundException;
import org.apache.streamline.cache.services.Service;
import org.apache.streamline.cache.services.io.CacheLoader;
import org.apache.streamline.cache.services.io.CacheReader;
import org.apache.streamline.cache.view.io.writer.CacheWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

public class CacheManagerImpl<K,V> implements AutoCloseable {
    private String cmId;    // Cache Manager Id
    private ConcurrentMap<String, Cache<K,V>> caches = new ConcurrentHashMap<>();

    Key<K> key;
    Val<K> val;

    Class<K> keyClass;
    Class<K> valClass;

    Collection<? extends Service> services;

    interface Key<K> {
        Class<K> getType();
    }

    interface Val<K> {
        Class<K> getType();
    }

     interface Codec<I, K> {
        K decode(I input);

        I encode(K key);
    }

        enum Status {STARTED, CLOSED}

        Cache<K, V> createCache(String cacheId, CacheConfiguration<K, V> config);

        <K,V> Cache<K, V> createCache(String cacheId, CacheBuilderComplex<K,V> cacheBuilder) {

        }


    public Cache<K, V> createAndRegisterCache(String cacheId, CacheBuilderComplex<K,V> cacheBuilder) {

    }

    public Cache<K, V> createAndRegisterCache(String cacheId, CacheConfig cacheConfig) {

    }

    private static final Logger LOG = LoggerFactory.getLogger(CacheManagerImpl.class);


    public void addCache(String cacheId, Cache<K,V> cache) {
        if (caches.containsKey(cacheId)) {
            throw new CacheAlreadyExistsException(String.format("Cache with id [%s] already exists in cache manager with id [%s]", cacheId, cmId));
        }
        caches.put(cacheId, cache);
    }

    public Cache<K, V> getCache(String cacheId) {
        if (!caches.containsKey(cacheId)) {
            throw new CacheNotFoundException(String.format("No cache with id [%s] found in cache manager with id [%s]", cacheId, cmId));
        }
        return caches.get(cacheId);
    }

    /**
     * closes and removes cache from this manager
     */
    public void removeCache(String cacheId) {
        if (!caches.containsKey(cacheId)) {
            LOG.warn("No cache with id [{}] found in cache manager with id [{}]", cacheId, cmId);
        } else {
            caches.get(cacheId).cl
            caches.remove(cacheId);
        }
    }


        boolean isLoader();

        Cache<K, V> createCache(String cacheId, Builder<? extends CacheConfiguration<K, V>> configBuilder);




        public void start() {
            for (Service service : services) {

            }
        }

        private void execute(Consumer cons) {
            cons.accept(null);
        }

        public void stop() {

        }

        @Override
        public void close() {

        }

        Status getStatus();

        interface CacheManagerBuilder {
            abstract CacheBuilderComplex withLoader(CacheLoader cacheLoader);

            abstract CacheBuilderComplex withReader(CacheReader cacheReader);

            abstract CacheBuilderComplex withWriter(CacheWriter cacheWriter);

            CacheManagerImpl build();

            abstract CacheBuilderComplex withExpiry();


            CacheManagerBuilder addCache(Cache cache);

            CacheManagerBuilder addCache(CacheBuilderComplex cache);


        }

    class LoaderDecorator {
        Cache cache;
    }

    class writerDecorator {
        Cache cache;
    }

    class readerDecorator {
        Cache cache;

        get(Function<>)

        get(cache);

        getAll();

    }
}
