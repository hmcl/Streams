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

package com.hortonworks.iotas.cache.view.loader;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.view.datastore.DataStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

    public Map<K, V> loadAll(final Collection<? extends K> keys, CacheLoaderListener<K,V> listener) {
        K key = getKey();
        try {
            List<Future<Map<K, V>>> futures = executorService.invokeAll(buildCallables(keys));
            listener.onCacheLoaded(getLoaded(futures));
        } catch (InterruptedException e) {
            listener.onCacheLoadingException(e);
            LOG.error("Failed to load keys [" + keys + "]", e);
        }
    }

    private Map<K, V> getLoaded(List<Future<Map<K, V>>> futures) throws ExecutionException, InterruptedException {
        Map<K,V> loaded = new HashMap<>();
        for (Future<Map<K, V>> future : futures) {
            if (future.isDone()) {
                future.get();
            }
        }
    }

    K getKey() {
        return null;
    }

    private Collection<? extends Callable<Map<K,V>>> buildCallables(final Collection<? extends K> keys) {
        Collection<? extends Callable<Map<K,V>>> callables = new ArrayList<>(keys.size());
        for (final K key : keys) {
            callables.add(new Callable<Map<K, V>>() {
                @Override
                public Map<K, V> call() throws Exception {
                    cache.putAll(key, dataStore.readAll(keys));

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

    public ExecutorService getExecutorService() {
        return executorService;
    }

    ListeningExecutorService service = MoreExecutors.listeningDecorator(executorService);
    ListenableFuture myCall = service.submit(new MyCallable(i));
    Futures.addCallback(myCall, new MyFutureCallback(i));
}
