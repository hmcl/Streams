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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CacheLoaderAsync<K,V> extends CacheLoader<K,V> {
    private static final int DEFAULT_NUM_THREADS = 5;
    private static final Logger LOG = LoggerFactory.getLogger(CacheLoaderAsync.class);

    private final ExecutorService executorService;

    public CacheLoaderAsync(Cache<K, V> cache, DataStore<K,V> dataStore) {
        this(cache, dataStore, Executors.newFixedThreadPool(DEFAULT_NUM_THREADS));
    }

    public CacheLoaderAsync(Cache<K, V> cache, DataStore<K,V> dataStore, ExecutorService executorService) {
        super(cache, dataStore);
        this.executorService = executorService;
    }

    public void loadAll(final Collection<? extends K> keys) {
        try {
            executorService.invokeAll(buildCallables(keys));
        } catch (InterruptedException e) {
            LOG.error("Failed to load keys [" + keys + "]", e);
        }
    }

    private Collection<? extends Callable<Map<K,V>>> buildCallables(Collection<? extends K> keys) {
        Collection<? extends Callable<Map<K,V>>> callables = new ArrayList<>(keys.size());
        for (final K key : keys) {
            callables.add(new Callable<Map<K, V>>() {
                @Override
                public Map<K, V> call() throws Exception {
                    cache.put(key, dataStore.readAll());

                }
            })
        }
        executorService.
    }

    private class Task implements Callable<Map<K,V>> {
        @Override
        public Map<K, V> call() throws Exception {
            return null;
        }
    }


}
