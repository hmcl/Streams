/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.hortonworks.iotas.cache.view.service;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.view.config.ExpiryPolicy;
import com.hortonworks.iotas.cache.view.config.Type;
import com.hortonworks.iotas.cache.view.datastore.DataStore;
import com.hortonworks.iotas.cache.view.datastore.writer.DataStoreWriter;
import com.hortonworks.iotas.cache.view.loader.CacheLoader;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CacheService<K,V> {
    protected ConcurrentMap<String, Cache<K,V>> caches = new ConcurrentHashMap<>();

    private String id;
    private Type.Cache cacheType;

    protected CacheLoader<K, V> cacheLoader;
    protected DataStoreWriter<K, V> dataStoreWriter;
    protected DataStore<K, V> dataStore;
    protected ExpiryPolicy expiryPolicy;  // ExpiryPolicy used by all the caches registered, if not overridden for a particular cache

    public CacheService(String id, Type.Cache cacheType) {
        this.id = id;
        this.cacheType = cacheType;
    }

    protected CacheService(Builder<K,V> builder) {
        this.id = builder.id;
        this.cacheType = builder.cacheType;
        this.cacheLoader = builder.cacheLoader;
        this.dataStoreWriter = builder.dataStoreWriter;
        this.dataStore = builder.dataStore;
        this.expiryPolicy= builder.expiryPolicy;
    }

    public static class Builder<K,V> {
        private final String id;
        private final Type.Cache cacheType;
        private ExpiryPolicy expiryPolicy;
        private CacheLoader<K, V> cacheLoader;
        private DataStoreWriter<K, V> dataStoreWriter;
        private DataStore<K, V> dataStore;

        public Builder(String id, Type.Cache cacheType) {
            this.id = id;
            this.cacheType= cacheType;
        }

        public Builder<K,V> setCacheLoader(CacheLoader<K, V> cacheLoader) {
            this.cacheLoader = cacheLoader;
            return this;
        }

        public Builder<K,V> setDataStoreWriter(DataStoreWriter<K, V> dataStoreWriter) {
            this.dataStoreWriter = dataStoreWriter;
            return this;
        }

        public Builder<K,V> setDataStore(DataStore<K, V> dataStore) {
            this.dataStore = dataStore;
            return this;
        }

        /**
         * Sets the {@link ExpiryPolicy} used by all the caches registered, if not overridden for a particular cache
         */
        public Builder<K,V> setExpiryPolicy(ExpiryPolicy expiryPolicy) {
            this.expiryPolicy = expiryPolicy;
            return this;
        }

        public CacheService<K,V> build() {
            return new CacheService<>(this);
        }
    }

    public <T extends Cache<K,V>> T getCache(String namespace) {
        return (T) caches.get(namespace);
    }

    public void registerCache(String namespace, Cache<K,V> cache) {
        caches.put(namespace, cache);
    }

    public String getServiceId() {
        return id;
    }

    public Type.Cache getCacheType() {
        return cacheType;
    }

    public CacheLoader<K, V> getCacheLoader() {
        return cacheLoader;
    }

    public DataStoreWriter<K, V> getDataStoreWriter() {
        return dataStoreWriter;
    }

    public DataStore<K, V> getDataStore() {
        return dataStore;
    }

    public Set<String> getCacheId() {
        return caches.keySet();
    }

    public ExpiryPolicy getExpiryPolicy() {
        return expiryPolicy;
    }

    /**
     * @return true if the {@link Cache} is backed by a {@link DataStore}, false otherwise
     */
    public boolean isDataStoreBacked() {
        return dataStore != null || dataStoreWriter != null || cacheLoader != null;
    }
}
