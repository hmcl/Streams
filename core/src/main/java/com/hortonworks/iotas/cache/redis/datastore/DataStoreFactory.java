package com.hortonworks.iotas.cache.redis.datastore;

public interface DataStoreFactory<K,V> {
    public DataStore<K,V> createDataStore();
}
