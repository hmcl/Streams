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

package com.hortonworks.iotas.cache.redis;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.redis.datastore.writer.DataStoreWriter;
import com.hortonworks.iotas.cache.redis.loader.CacheLoader;
import com.hortonworks.iotas.cache.stats.CacheStats;
import com.hortonworks.iotas.storage.exception.StorageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DataStoreBackedCache<K,V> implements Cache<K,V> {
    private static final Logger LOG = LoggerFactory.getLogger(DataStoreBackedCache.class);
    private final Cache<K, V> cache;
    private final CacheLoader<K, V> cacheLoader;
    private final DataStoreWriter<K, V> dataStoreWriter;

    public DataStoreBackedCache(Cache<K,V> cache, CacheLoader<K,V> cacheLoader, DataStoreWriter<K,V> dataStoreWriter) {

        this.cache = cache;
        this.cacheLoader = cacheLoader;
        this.dataStoreWriter = dataStoreWriter;
    }

    @Override
    public V get(K key) throws StorageException {
        V val = cache.get(key);
        if (val == null) {
            val = cacheLoader.load(key);
        }
        return val;
    }

    @Override
    public Map<K, V> getAllPresent(Collection<? extends K> keys) {
        Map<K, V> allPresent = cache.getAllPresent(keys);
        if (allPresent == null || allPresent.isEmpty() || allPresent.size() < keys.s) {

        }


        final Map<K, V> existing = new HashMap<>();
        final List<K> nonExisting = new LinkedList<>();

        cac

        for (K key : keys) {
            final V val = get(key);
            if (val != null) {
                existing.put(key, val);
            } else {
                nonExisting.add(key);
            }
        }
        LOG.debug("Entries existing in cache [{}]. Keys non existing in cache: [{}]", existing, nonExisting);
        return existing;
    }

    @Override
    public void put(K key, V val) {
        redisConnection.set(key, val);
        LOG.debug("Set {}=>{}", key, val);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        redisConnection.mset(entries);
    }

    @Override
    public void remove(K key) {
        redisConnection.del(key);
    }

    @Override
    public Map<K, V> removeAllPresent(Iterable<? extends K> keys) {
        return null;
    }

    @Override
    public void clear() {
        Collection<? extends K> keys = new ArrayList<>();
        removeAllPresent(keys);

        Map<? extends Number, ? extends Number> mnn = null;
        Map<Integer, Integer> mii = null;
        mnn = mii;

    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public CacheStats stats() {
        return null;
    }

    public static class Builder<K,V> {
        private static final long DEFAULT_MAX_BYTES = 10*1024*1024;     // 10 MBs

        public Builder() { }

        private long sizeBytes = DEFAULT_MAX_BYTES;
        private BytesCalculator bytesCalculator;
        private long maxSizeBytes;

        public Builder setMaxSizeBytes(long maxSizeBytes) {
            this.maxSizeBytes = maxSizeBytes;
            return this;
        }

        public Builder setMaxSizeBytesConverter(BytesCalculator bytesCalculator) {
            this.bytesCalculator = bytesCalculator;
            return this;
        }

        public Cache<K,V> build() {
            if (bytesCalculator != null) {
                LOG.debug("Setting ");

            }
            return null;    //TODO
        }
     }




    public interface BytesCalculator<T> {
        /**
         * @param object object that can be used to calculate the number of bytes that should be used to expire the cache
         * @return bytes
         */
        long computeBytes(T object);
    }


}
