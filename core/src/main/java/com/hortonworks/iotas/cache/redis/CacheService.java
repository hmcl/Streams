package com.hortonworks.iotas.cache.redis;

import com.hortonworks.iotas.cache.Cache;

public class CacheService {
    private final Cache cache;
    private final DataStoreReader mapDataStore;

    public CacheService(CacheServiceFactory cacheServiceFactory) {
        this.cache = cacheServiceFactory.createCache();
        this.mapDataStore = cacheServiceFactory.createMapDataStore();
    }

    public Cache getCache() {
        return cache;
    }

    public DataStoreReader getMapDataStore() {
        return mapDataStore;
    }
}
