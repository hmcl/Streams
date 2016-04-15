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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
    public void put(K key, V val) {
        cache.put(key, val);
        dataStoreWriter.write(key, val);
    }

    @Override
    public Map<K, V> getAll(Collection<? extends K> keys) {  // TODO what if trying to load more keys than max number of keys that be kept in the cache ?
        Map<K, V> present = cache.getAll(keys);
        if (present == null || present.isEmpty()) {
            present = cacheLoader.loadAll(keys);
        } else if (present.size() < keys.size()) {
            Set<K> notPresent = new HashSet<>(keys);
            notPresent.removeAll(present.keySet());
            Map<K, V> loaded = cacheLoader.loadAll(notPresent);     //TODO handle NPE
            present.putAll(loaded);
        }
        LOG.debug("Entries existing in cache [{}]. Keys non existing in cache: [{}]", present, notPresent.removeAll(loaded));
        return present;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        cache.putAll(entries);
        dataStoreWriter.writeAll(entries);
    }

    @Override
    public void remove(K key) {
        cache.remove(key);
        dataStoreWriter.delete(key);
    }

    @Override
    public void removeAll(Collection<? extends K> keys) {
        cache.removeAll(keys);
        dataStoreWriter.deleteAll(keys);
    }

    @Override
    public void clear() {
        cache.clear();
        LOG.warn("Entries only removed from cache but not from DB");    //TODO Remove from cache
    }

    @Override
    public long size() {
        return cache.size();
    }

    @Override
    public CacheStats stats() {
        return null;
    }
}
