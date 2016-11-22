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

package org.apache.streamline.cache.services.io;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.streamline.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CacheLoaderAsync<K,V> implements CacheLoader<K,V> {
    private static final int DEFAULT_NUM_THREADS = 5;
    private static final Logger LOG = LoggerFactory.getLogger(CacheLoaderAsync.class);

    private final Cache<K,V> cache;
    private final CacheReader<K, V> cacheReader;
    private final ListeningExecutorService executorService;

    public CacheLoaderAsync(Cache<K, V> cache, CacheReader<K,V> cacheReader) {
        this(cache, cacheReader, Executors.newFixedThreadPool(DEFAULT_NUM_THREADS, new CacheLoaderThreadFactory()));
    }

    public CacheLoaderAsync(Cache<K, V> cache, CacheReader<K,V> cacheReader, ExecutorService executorService) {
        this.cache = cache;
        this.cacheReader = cacheReader;
        this.executorService = MoreExecutors.listeningDecorator(executorService);
    }

    public void loadAll(final Collection<? extends K> keys, CacheLoader.Listener<K,V> listener) {
        try {
            ListenableFuture<Map<K,V>> future = executorService.submit(new DataStoreCallable(keys));
            Futures.addCallback(future, new CacheLoaderAsyncFutureCallback(keys, listener));
        } catch (Exception e) {
            final String msg = String.format("Exception occurred while loading keys [%s]", keys);
            LOG.error(msg, e);
            listener.onCacheLoadingFailure(new Exception(msg, e));
        }
    }

    private static class CacheLoaderThreadFactory implements ThreadFactory {
        private static final String CACHE_LOADER_THREAD = "cache-loader-thread-";
        private static AtomicInteger count = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, CACHE_LOADER_THREAD + count.getAndIncrement());
            }
    }

    private class DataStoreCallable implements Callable<Map<K,V>> {
        private Collection<? extends K> keys;

        public DataStoreCallable(Collection<? extends K> keys) {
            this.keys = keys;
        }

        @Override
        public Map<K, V> call() throws Exception {
            final Map<K, V> result = cacheReader.readAll(keys);
            LOG.debug("Reading keys [{}] from data store returned [{}]", keys, result);
            return result;
        }
    }

    private class CacheLoaderAsyncFutureCallback implements FutureCallback<Map<K,V>> {
        private final Collection<? extends K> keys;
        private CacheLoader.Listener<K,V> listener;

        public CacheLoaderAsyncFutureCallback(Collection<? extends K> keys, CacheLoader.Listener<K, V> listener) {
            this.keys = keys;
            this.listener = listener;
        }

        @Override
        public void onSuccess(Map<K, V> read) {
            LOG.debug("Reading keys [{}] from data store returned [{}]", keys, read);
            final Map<K,V> loaded = new HashMap<>();

            if (read != null) {
                for (Map.Entry<K, V> re : read.entrySet()) {
                    if (re.getKey() != null && re.getValue() != null) {
                        loaded.put(re.getKey(), re.getValue());
                    } else {
                        LOG.trace("Not loading into cache entry with null key or value [{}]", re);
                    }
                }
            }

            cache.putAll(loaded);
            LOG.debug("Loaded cache with entries [{}]", loaded);
            listener.onCacheLoaded(loaded);
        }

        @Override
        public void onFailure(Throwable t) {
            listener.onCacheLoadingFailure(t);
        }
    }

    @Override
    public void init() {

    }

    @Override
    public void close() throws Exception {

    }
}
