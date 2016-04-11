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

package com.hortonworks.iotas.cache.redis.service;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.redis.datastore.DataStore;
import com.hortonworks.iotas.cache.redis.datastore.DataStoreReader;
import com.hortonworks.iotas.cache.redis.loader.CacheLoader;

import java.util.Collection;

public class CacheService<K,V> {
    private final Cache<K,V> cache;
    private DataStore<K,V> dataStore;
    private CacheLoader<K,V> cacheLoader;

    public CacheService(CacheServiceFactory<K,V> factory) {
        this.cache = factory.createCache();
        this.dataStore = factory.createDataStore();
    }

    public Cache<K,V> getCache() {
        return cache;
    }

    public DataStore<K,V> getDataStore() {
        return dataStore;
    }

    public V load(K key) {
        return cacheLoader.load(key);
    }

    public void loadAll(Collection<? extends K> keys) {

    }


}
