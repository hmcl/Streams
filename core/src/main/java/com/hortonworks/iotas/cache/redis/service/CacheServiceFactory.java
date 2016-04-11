package com.hortonworks.iotas.cache.redis.service;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.redis.datastore.DataStore;
import com.hortonworks.iotas.cache.redis.datastore.DataStoreReader;

public interface CacheServiceFactory<K,V> {

    Cache<K,V> createCache();

    DataStore<K,V> createDataStore();


 }
