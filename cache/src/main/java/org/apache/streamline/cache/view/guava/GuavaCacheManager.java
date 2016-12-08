package org.apache.streamline.cache.view.guava;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheBuilderSpec;
import com.google.common.cache.CacheLoader;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.config.builder.CacheConfig;
import org.apache.streamline.cache.decorators.LoadableCache;
import org.apache.streamline.cache.decorators.WriteThroughCache;
import org.apache.streamline.cache.manager.CacheManager;
import org.apache.streamline.cache.manager.LocalCacheManager;
import org.apache.streamline.cache.services.io.CacheReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GuavaCacheManager extends LocalCacheManager implements CacheManager {
    private static final Logger LOG = LoggerFactory.getLogger(GuavaCacheManager.class);

    @Override
    public <K, V> Cache<K, V> createCache(String cacheId, CacheConfig<K, V> config) {
        try {
            addConfig(cacheId, config);
            addServices(cacheId, config);
            final Cache<K, V> cacheView = createCacheFromConfig(config);
            addCache(cacheId, cacheView);
            LOG.info("Created cache id [{}], config [{}], view instance [{}]", cacheId, config, cacheView.getClass().getSimpleName());
            return cacheView;
        } catch (Exception e) {
            LOG.error("Exception occurred while creating cache with id [{}] and config [{}]", cacheId, config, e);
            removeCache(cacheId);
            removeServices(cacheId);
            removeConfig(cacheId);
            throw e;
        }
    }


    private <K, V> Cache<K, V> createCacheFromConfig(final CacheConfig<K, V> config) {
        @SuppressWarnings("unchecked")
        final CacheBuilder<K, V> builder = (CacheBuilder<K, V>) CacheBuilder.from(config.<CacheBuilderSpec>getDelegateCacheConfig());
        com.google.common.cache.Cache<K, V> guavaCache;

        if (config.isReadable()) {
            guavaCache = builder.build(new CacheLoader<K, V>() {
                final CacheReader<K, V> reader = config.getReader();
                @Override
                public V load(K key) throws Exception {
                    return reader.read(key);
                }
            });
        } else {
            guavaCache = builder.build();
        }

        Cache<K, V> cacheView = new GuavaCache<>(guavaCache);

        if (config.isLoadable()) {
            cacheView = new LoadableCache<>(cacheView, config.getLoader());
        }

        if (config.isWritable()) {
            cacheView = new WriteThroughCache<>(cacheView, config.getWriter());
        }

        return cacheView;
    }
}
