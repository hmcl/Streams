package com.hortonworks.iotas.cache.redis;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.redis.datastore.DataStoreReader;

public class CacheManager<K,V> {
    DataStoreReader<K,V> dataStore;
    Cache<K,V> cache;


}
