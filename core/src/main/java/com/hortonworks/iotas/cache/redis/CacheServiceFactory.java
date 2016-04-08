package com.hortonworks.iotas.cache.redis;

import com.hortonworks.iotas.cache.Cache;

public interface CacheServiceFactory {

    Cache createCache();

    <T extends DataStoreReader> T createDataStore();
 }
