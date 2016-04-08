package com.hortonworks.iotas.cache.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class CacheServiceRegistry {
    protected static final Logger LOG = LoggerFactory.getLogger(CacheServiceRegistry.class);

    private CacheServiceRegistry instance = new CacheServiceRegistry();

    private Map<CacheServiceId, CacheService> idToService;


    private CacheServiceRegistry() {
        idToService = new HashMap<>();
    }

    public CacheServiceRegistry getInstance() {
        return instance;
    }

    public void register(CacheServiceId cacheServiceId, CacheService cacheService) {
        idToService.put(cacheServiceId, cacheService);
        LOG.info("Registered {} as {}.", cacheService, cacheServiceId);
    }

    public CacheService getCacheService(CacheServiceId cacheServiceId) {
        return idToService.get(cacheServiceId);
    }
}
