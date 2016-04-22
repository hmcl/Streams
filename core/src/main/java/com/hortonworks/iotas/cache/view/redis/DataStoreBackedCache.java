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

package com.hortonworks.iotas.cache.view.redis;

import com.hortonworks.iotas.cache.AbstractCache;
import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.stats.CacheStats;
import com.hortonworks.iotas.cache.view.datastore.DataStore;
import com.hortonworks.iotas.cache.view.datastore.writer.DataStoreWriter;
import com.hortonworks.iotas.cache.view.loader.CacheLoader;
import com.hortonworks.iotas.storage.exception.StorageException;
import com.sun.javaws.exceptions.InvalidArgumentException;
import com.sun.org.apache.bcel.internal.generic.DASTORE;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

public class DataStoreBackedCache<K,V> extends AbstractCache<K,V> implements Cache<K,V> {
    private static final Logger LOG = LoggerFactory.getLogger(DataStoreBackedCache.class);
    private final Cache<K, V> cache;
    private final CacheLoader<K, V> cacheLoader;
    private final DataStoreWriter<K, V> dataStoreWriter;
    private final DataStore<K, V> dataStore;

    public DataStoreBackedCache(Cache<K,V> cache, DataStore<K, V> dataStore,
            CacheLoader<K,V> cacheLoader, DataStoreWriter<K, V> dataStoreWriter) {

        validateArguments(cache, dataStore, cacheLoader, dataStoreWriter);

        this.cache = cache;
        this.dataStore = dataStore;
        this.cacheLoader = cacheLoader;
        this.dataStoreWriter = dataStoreWriter;
    }

    interface CacheLoaderListener<K,V> {
        void onCacheLoaded(Future<Map<K,V>> loaded);
        void onCacheLoadingException();
    }

    public void loadAll(Collection<? extends K> keys) {
        cacheLoader.loadAll(keys);
    }

    public void loadAllAsync(Collection<? extends K> keys, CacheLoaderListener listener) {
        cacheLoader.loadAll(keys);
    }

    @Override
    public V get(K key) throws StorageException {
        V val = cache.get(key);
        if (val == null && dataStore != null) {     // in sync read through
            val = dataStore.read(key);
        }
        return val;
    }

    @Override
    public void put(K key, V val) {
        cache.put(key, val);
        if (dataStoreWriter != null) {              // in sync write through
            dataStoreWriter.write(key, val);
        }
    }

    @Override
    public Map<K, V> getAll(Collection<? extends K> keys) {  // TODO what if trying to load more keys than max number of keys that be kept in the cache ?
        Map<K, V> present = cache.getAll(keys);
        if (dataStore != null) {
            if (present == null || present.isEmpty()) {
                present = dataStore.readAll(keys);                  // in sync read through
            } else if (present.size() < keys.size()) {
                Set<K> notPresent = new HashSet<>(keys);
                notPresent.removeAll(present.keySet());
                Map<K, V> loaded = dataStore.readAll(notPresent);   // in sync read through
                present.putAll(loaded);
            }
        }
//        LOG.debug("Entries existing in cache [{}]. Keys non existing in cache: [{}]", present, notPresent.removeAll(loaded));
        return present;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        cache.putAll(entries);
        if (dataStoreWriter != null) {      // sync or async write, depending on the writing strategy chosen
            dataStoreWriter.writeAll(entries);
        }
    }

    @Override
    public void remove(K key) {
        cache.remove(key);
        if (dataStoreWriter != null) {      // sync or async delete, depending on the writing strategy chosen
            dataStoreWriter.delete(key);
        }
    }

    @Override
    public void removeAll(Collection<? extends K> keys) {
        cache.removeAll(keys);
        if (dataStoreWriter != null) {      // sync or async delete, depending on the writing strategy chosen
            dataStoreWriter.deleteAll(keys);
        }
    }

    @Override
    public void clear() {
        cache.clear();
        LOG.info("Entries only removed from cache but not from backing data store");    //TODO: Do we want to remove from DB as well ?
    }

    @Override
    public long size() {
        return cache.size();
    }

    @Override
    public CacheStats stats() {
        return cache.stats();
    }

    @Override
    public String toString() {
        return "DataStoreBackedCache{" +
                "cache=" + cache +
                ", cacheLoader=" + cacheLoader +
                ", dataStoreWriter=" + dataStoreWriter +
                ", dataStore=" + dataStore +
                "} " + super.toString();
    }

    // =========== Private helper methods ===========

    private void validateArguments(Cache<K, V> cache, DataStore<K, V> dataStore,
                                   CacheLoader<K, V> cacheLoader, DataStoreWriter<K, V> dataStoreWriter) {
        if (cache == null) {
            throw new IllegalArgumentException("Cache reference cannot be null");
        }

        if (dataStore == null && cacheLoader == null && dataStoreWriter == null) {
            throw new IllegalArgumentException(String.format("At least one non null implementation of %s,  %s, or  %s " +
                            "is required. If no backing data store is required consider using a non backed implementation of %s",
                    getSimpleName(DataStore.class), getSimpleName(DataStoreWriter.class),
                    getSimpleName(CacheLoader.class), getSimpleName(Cache.class)));
        }

        LOG.info("Created {}", this);
    }

    private String getSimpleName(Class<?> clazz) {
        return clazz.getSimpleName();
    }

}
