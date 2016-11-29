package org.apache.streamline.cache.config.builder;

import com.google.common.cache.CacheBuilder;

import org.apache.streamline.cache.services.io.CacheLoader;
import org.apache.streamline.cache.util.Factory;

public class CacheConfigBuilder<K,V> {
    private String cacheId;
    private CacheConfig cacheConfig;
    private Expiry expiry;
    private Eviction<?> eviction;

    public CacheConfigBuilder(String cacheId) {
        this.cacheId = cacheId;
    }

    /**
     * inherits from given config
     */
    CacheConfigBuilder fromConfig(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
        return this;
    }

    CacheConfigBuilder withLoaderFactory(Factory<CacheLoader> loaderFactory) {
        return this;
    }

    CacheConfigBuilder withLoader(CacheLoader loader) {
        return this;
    }

    CacheConfigBuilder withReader(){
        return this;
    }

    CacheConfigBuilder withWriter(){
        return this;
    }

    CacheConfigBuilder withEviction(Eviction eviction){
        this.eviction = eviction;
        return this;
    }

    CacheConfigBuilder withExpiry(Expiry expiry){
        this.expiry = expiry;
        return this;
    }

    CacheConfig<K,V> build() {
        return new CacheConfig<K, V>();
    }

    void m() {
        CacheBuilder.newBuilder().
    }

}
