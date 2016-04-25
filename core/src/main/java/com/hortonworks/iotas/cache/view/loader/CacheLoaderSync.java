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

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.view.datastore.DataStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class CacheLoaderSync<K,V> extends CacheLoader<K,V> {
    private static final Logger LOG = LoggerFactory.getLogger(CacheLoaderSync.class);

    public CacheLoaderSync(Cache<K, V> cache, DataStore<K,V> dataStore) {
        super(cache, dataStore);
    }

    public void loadAll(Collection<? extends K> keys, CacheLoaderListener<K,V> listener) {
        Map<K, V> entries;
        try {
            entries = dataStore.readAll(keys);
            cache.putAll(entries);
            listener.onCacheLoaded(entries);
        } catch (Exception e) {
            final String msg = String.format("Exception occurred while loading keys [%s]", keys);
            listener.onCacheLoadingException(new Exception(msg, e));
            LOG.error(msg, e);
        }
    }
}
