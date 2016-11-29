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

package org.apache.streamline.cache;

import org.apache.streamline.cache.exception.CacheException;
import org.apache.streamline.cache.services.io.CacheLoader;
import org.apache.streamline.cache.services.io.CacheReader;
import org.apache.streamline.cache.services.io.CacheWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ReadWriteLoadableCache<K,V> implements LoadableCache<K,V> {
    private static final Logger LOG = LoggerFactory.getLogger(ReadWriteLoadableCache.class);

    private final LoadableCache<K, V> cacheView;
    private final CacheLoader<K, V> cacheLoader;
    private final CacheWriter<K, V> cacheWriter;
    private final CacheReader<K, V> cacheReader;

    public ReadWriteLoadableCache(LoadableCache<K, V> cacheView, CacheLoader<K, V> cacheLoader,
                                  CacheReader<K, V> cacheReader, CacheWriter<K, V> cacheWriter) {

        validateArguments(cacheView, cacheReader, cacheLoader, cacheWriter);

        this.cacheView = cacheView;
        this.cacheReader = cacheReader;
        this.cacheLoader = cacheLoader;
        this.cacheWriter = cacheWriter;

        LOG.info("Created {}", this);
    }

    public void loadAll(Collection<? extends K> keys, CacheLoader.Listener<K,V> listener) {
        if (cacheLoader != null) {
            cacheLoader.loadAll(keys, listener);
            LOG.debug("Entries associated with keys [{}] successfully loaded into cache", keys);
        } else {
            LOG.info("No cache loader set. Not loading keys [{}]", keys);
        }
    }

    @Override
    public V get(K key) {
        V val = cacheView.get(key);
        if (val == null && cacheReader != null) {     // cache miss
            val = cacheReader.read(key);              // in sync read through
            cacheView.put(key, val);                  // load cache with value read from storage
        }
        return val;
    }

    @Override
    public Map<K, V> getAll(Collection<? extends K> keys) {  // TODO what if trying to load more keys than max number of keys that be kept in the cache ?
        Map<K, V> present = cacheView.getAll(keys);
        LOG.debug("Entries existing in cache [{}].", present);

        if (cacheReader != null) {
            if (present == null || present.isEmpty()) {
                present = cacheReader.readAll(keys);                  // in sync read through
                cacheView.putAll(present);                            // load cache with values read from storage
            } else if (present.size() < keys.size()) {                // not all values are in cache, i.e., some cache misses
                final Set<K> notPresent = new HashSet<>(keys);
                notPresent.removeAll(present.keySet());
                final Map<K, V> loaded = cacheReader.readAll(notPresent);   // in sync read through
                present.putAll(loaded);
                cacheView.putAll(loaded);                                           // load cache with values read from storage
                LOG.debug("Keys non existing in cache: [{}]. Entries read from database and loaded into cache [{}]", notPresent, loaded);
            }
            present = cacheView.getAll(keys);
        }
        LOG.debug("Entries existing in cache after loading [{}]", cacheView.getAll(keys));
        return present;
    }

    @Override
    public void put(K key, V val) {
        if (cacheWriter != null) {
            cacheWriter.write(key, val);        // in sync write through
        }
        cacheView.put(key, val);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        if (cacheWriter != null) {      // sync or async write, depending on the writing strategy chosen
            cacheWriter.writeAll(entries);
        }
        cacheView.putAll(entries);
    }

    @Override
    public void remove(K key) {
        if (cacheWriter != null) {      // sync or async delete, depending on the writing strategy chosen
            cacheWriter.delete(key);
        }
        cacheView.remove(key);
    }

    @Override
    public void removeAll(Collection<? extends K> keys) {
        if (cacheWriter != null) {      // sync or async delete, depending on the writing strategy chosen
            cacheWriter.deleteAll(keys);
        }
        cacheView.removeAll(keys);
    }

    @Override
    public void clear() {
        cacheView.clear();
        LOG.info("Cache cleared. Entries only removed from cache but not from backing storage");    //TODO: Do we want to remove from DB as well ?
    }

    @Override
    public long size() {
        return cacheView.size();
    }

    @Override
    public <S> S stats() {
        return cacheView.stats();
    }

    @Override
    public void init() {

    }

    @Override
    public void close() throws CacheException {

    }

    @Override
    public String toString() {
        return "ReadWriteLoadableCache{" +
                "cacheView=" + cacheView +
                ", cacheLoader=" + cacheLoader +
                ", cacheWriter=" + cacheWriter +
                ", cacheReader=" + cacheReader +
                '}';
    }

    // =========== Private helper methods ===========

    private void validateArguments(Cache<K, V> cache, CacheReader<K, V> cacheReader,
                                   CacheLoader<K, V> cacheLoader, CacheWriter<K, V> cacheWriter) {
        if (cache == null) {
            throw new IllegalArgumentException("Cache reference cannot be null");
        }

        if (cacheReader == null && cacheLoader == null && cacheWriter == null) {
            throw new NullPointerException(String.format("At least one non null implementation of %s,  %s, or  %s " +
                            "is required. If no backing storage is required consider using a non backed implementation of %s",
                    getSimpleName(CacheReader.class), getSimpleName(CacheWriter.class),
                    getSimpleName(CacheLoader.class), getSimpleName(Cache.class)));
        }
    }

    private String getSimpleName(Class<?> clazz) {
        return clazz.getSimpleName();
    }
}
