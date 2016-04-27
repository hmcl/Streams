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
import com.hortonworks.iotas.cache.view.DataStoreBackedCache;
import com.hortonworks.iotas.cache.view.config.ExpiryPolicy;
import com.hortonworks.iotas.cache.view.config.TypeConfig;
import com.hortonworks.iotas.cache.view.datastore.DataStoreReader;
import com.hortonworks.iotas.cache.view.datastore.DataStoreWriter;
import com.hortonworks.iotas.cache.view.io.loader.CacheLoader;
import com.hortonworks.iotas.cache.view.io.writer.CacheWriter;

public class DataStoreBackedCacheService<K,V> extends CacheService<K,V> {
    protected CacheLoader<K, V> cacheLoader;            // used to load cache sync or async
    protected CacheWriter<K, V> cacheWriter;            // used to write to db sync or async
    protected DataStoreReader<K, V> dataStoreReader;    // used for read through

    public DataStoreBackedCacheService(String id, TypeConfig.Cache cacheType) {
        super(id, cacheType);
    }

    protected DataStoreBackedCacheService(Builder<K, V> builder) {
        super(builder);
        this.cacheLoader = builder.cacheLoader;
        this.cacheWriter = builder.cacheWriter;
        this.dataStoreReader = builder.dataStoreReader;
    }

    public static class Builder<K,V> extends CacheService.Builder<K,V> {
        private CacheLoader<K, V> cacheLoader;
        private CacheWriter<K, V> cacheWriter;
        private DataStoreReader<K, V> dataStoreReader;

        public Builder(String id, TypeConfig.Cache cacheType) {
            super(id, cacheType);
        }

        public Builder<K,V> setCacheLoader(CacheLoader<K, V> cacheLoader) {
            this.cacheLoader = cacheLoader;
            return this;
        }

        public Builder<K,V> setCacheWriter(CacheWriter<K, V> cacheWriter) {
            this.cacheWriter = cacheWriter;
            return this;
        }

        public Builder<K,V> setDataStoreReader(DataStoreReader<K, V> dataStoreReader) {
            this.dataStoreReader = dataStoreReader;
            return this;
        }

        public DataStoreBackedCacheService<K,V> build() {
            return new DataStoreBackedCacheService<>(this);
        }
    }

    public void registerCache(String id, Cache<K,V> cache) {
        if (isDataStoreBacked()) {
            caches.putIfAbsent(this.id, createDataStoreBackedCache(cache));
        } else {
            super.registerCache(id, cache);
        }
    }

    private DataStoreBackedCache<K, V> createDataStoreBackedCache(Cache<K,V> cache) {
        return new DataStoreBackedCache<>(cache, cacheLoader, dataStoreReader, cacheWriter);
    }

    public CacheLoader<K, V> getCacheLoader(String cacheId) {
        return cacheLoader;
    }

    public DataStoreWriter<K, V> getCacheWriter() {
        return cacheWriter;
    }

    public DataStoreReader<K, V> getDataStoreReader() {
        return dataStoreReader;
    }

    /**
     * @return true if the {@link Cache} is backed by a {@link DataStoreReader}, false otherwise
     */
    public boolean isDataStoreBacked() {
        return dataStoreReader != null || cacheWriter != null || cacheLoader != null;
    }
}
