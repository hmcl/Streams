package org.apache.streamline.cache;

public abstract class CacheBuilderComplex<K,V> {
    private String cacheId;

    /**
     * @param cacheId id of the cache. Must be unique
     */
    public CacheBuilderComplex(String cacheId) {
        this.cacheId = cacheId;
    }

    abstract CacheBuilderComplex withLoader();

    abstract CacheBuilderComplex withReader();

    abstract CacheBuilderComplex withWriter();

    abstract CacheBuilderComplex withExpiry();

    abstract CacheBuilderComplex<K,V> withConfig();  // configurable for cache type - Redis, Guava

    abstract <K,V> Cache<K,V> build();
}
