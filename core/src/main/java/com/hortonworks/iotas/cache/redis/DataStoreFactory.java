package com.hortonworks.iotas.cache.redis;

public interface DataStoreFactory<K, V> {
    public <T extends DataStoreFactory<K,V>> T createDataStore();
}
