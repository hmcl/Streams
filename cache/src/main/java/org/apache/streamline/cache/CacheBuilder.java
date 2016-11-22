package org.apache.streamline.cache;


import org.apache.streamline.cache.services.io.CacheLoader;

public interface CacheBuilder<T> {
    <K,V> CacheBuilder<T> withCacheLoader(CacheLoader<K,V> cacheLoader);

    CacheBuilder<T>  withLoader();

    CacheBuilder<T> withReader();

    CacheBuilder<T> withWriter();

    T build();
}
