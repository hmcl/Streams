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

package com.hortonworks.iotas.cache.redis.loader;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.redis.datastore.DataStore;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CacheLoaderAsync<K,V> extends CacheLoader<K,V> {
    private static final int DEFAULT_NUM_THREADS = 5;
    private final ExecutorService executorService;

    public CacheLoaderAsync(Cache<K, V> cache, DataStore<K,V> dataStore) {
        this(cache, dataStore, Executors.newFixedThreadPool(DEFAULT_NUM_THREADS));
    }

    public CacheLoaderAsync(Cache<K, V> cache, DataStore<K,V> dataStore, ExecutorService executorService) {
        super(cache, dataStore);
        this.executorService = executorService;
    }

    public void loadAll(final Collection<? extends K> keys) {
        executorService.submit(new Callable<Map<K, V>>() {
            @Override
            public Map<K, V> call() throws Exception {
                cache.putAll(dataStore.readAll(keys));
                return null;
            }
        });
    }
}
