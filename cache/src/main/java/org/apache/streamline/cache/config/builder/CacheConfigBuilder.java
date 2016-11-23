package org.apache.streamline.cache.config.builder;

import com.google.common.cache.CacheBuilder;

import org.apache.streamline.cache.services.io.CacheLoader;
import org.apache.streamline.cache.util.Factory;

public class CacheConfigBuilder {

    private CacheConfig cacheConfig;

    CacheConfigBuilder withCacheConfig(CacheConfig cacheConfig) {
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

    CacheConfigBuilder withEviction(){
        return this;
    }

    CacheConfigBuilder withExpiry(){
        return this;
    }

    void m() {
        CacheBuilder.newBuilder().
    }

    interface Expiry {
        void creation();

        void access();

        void update();
    }
}
