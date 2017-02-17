package org.apache.streamline.cache.view.guava;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheBuilderSpec;
import com.google.common.cache.CacheLoader;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.config.builder.CacheConfig;
import org.apache.streamline.cache.decorators.LoadableCache;
import org.apache.streamline.cache.decorators.WriteThroughCache;
import org.apache.streamline.cache.exception.CacheConfigException;
import org.apache.streamline.cache.manager.CacheManager;
import org.apache.streamline.cache.manager.LocalCacheManager;
import org.apache.streamline.cache.services.io.CacheReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GuavaCacheManager extends LocalCacheManager implements CacheManager {
    private static final Logger LOG = LoggerFactory.getLogger(GuavaCacheManager.class);

    @Override
    public <K, V, C> Cache<K, V> createCache(String cacheId, CacheConfig<K, V, C> config) {
        try {
            @SuppressWarnings("unchecked")
            final Cache<K, V> cacheView = createCacheFromConfig((CacheConfig<K, V, CacheBuilderSpec>) config);
            addCache(cacheId, cacheView, config);
            LOG.info("Created [cache={}] with [id={}] and [config={}]", cacheView, cacheId, config);
            return cacheView;
        } catch (Exception e) {
            LOG.error("Exception occurred while creating cache with [id={}] and [config={}]", cacheId, config, e);
            removeCache(cacheId);
            throw e;
        }
    }


    private <K, V> Cache<K, V> createCacheFromConfig(final CacheConfig<K, V, CacheBuilderSpec> config) {
        @SuppressWarnings("unchecked")
        final CacheBuilder<K, V> gcBuilder =(CacheBuilder<K, V>) CacheBuilder.from(config.getDelegateCacheConfig()
                .orElse(CacheBuilderSpec.parse("")));

        com.google.common.cache.Cache<K, V> guavaCache;

        if (config.isReadable()) {
            guavaCache = gcBuilder.build(new CacheLoader<K, V>() {
                final CacheReader<K, V> reader = config.getReader().get();
                @Override
                public V load(K key) throws Exception {
                    return reader.read(key);
                }
            });
        } else {
            guavaCache = gcBuilder.build();
        }

        Cache<K, V> cacheView = new GuavaCache<>(guavaCache);

        if (config.isLoadable()) {
            cacheView = new LoadableCache<>(cacheView, config.getLoader().orElseThrow(() ->
                    new CacheConfigException("Failed to create LoadableCache because CacheLoader not specified")));
        }

        if (config.isWritable()) {
            cacheView = new WriteThroughCache<>(cacheView, config.getWriter().orElseThrow(() ->
                    new CacheConfigException("Failed to create WritableCache because CacheWriter not specified")));
        }

        return cacheView;
    }
}
