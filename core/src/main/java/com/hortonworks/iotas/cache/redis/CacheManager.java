package com.hortonworks.iotas.cache.redis;

import com.hortonworks.iotas.cache.Cache;

public class CacheManager<K,V> {
    MapDataStore<K,V> dataStore;
    Cache<K,V> cache;


}
