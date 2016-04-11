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

public class CacheLoaderSync<K,V> extends CacheLoader<K,V> {
    public CacheLoaderSync(Cache<K, V> cache, DataStore<K,V> dataStore) {
        super(cache, dataStore);
    }

    public void loadAll(Collection<? extends K> keys) {
        cache.putAll(dataStore.readAll(keys));
    }
}
