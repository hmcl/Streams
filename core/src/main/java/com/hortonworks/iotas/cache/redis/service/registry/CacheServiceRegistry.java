package com.hortonworks.iotas.cache.redis.service.registry;

import com.hortonworks.iotas.cache.redis.service.CacheService;
import com.hortonworks.iotas.cache.redis.service.CacheServiceId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public enum CacheServiceRegistry {
    INSTANCE;

    protected static final Logger LOG = LoggerFactory.getLogger(CacheServiceRegistry.class);

    private Map<CacheServiceId, CacheService> idToService;

    CacheServiceRegistry() {
        idToService = new HashMap<>();
    }

    public void register(CacheServiceId cacheServiceId, CacheService cacheService) {
        idToService.put(cacheServiceId, cacheService);
        LOG.info("Registered {} as {}.", cacheService, cacheServiceId);
    }

    public CacheService getCacheService(CacheServiceId cacheServiceId) {
        return idToService.get(cacheServiceId);
    }
}
