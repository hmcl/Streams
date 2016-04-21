package com.hortonworks.iotas.cache.view.datastore;

public interface DataStoreFactory<K,V> {
    public DataStore<K,V> createDataStore();
}
