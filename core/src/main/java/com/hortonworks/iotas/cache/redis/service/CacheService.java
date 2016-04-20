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

package com.hortonworks.iotas.cache.redis.service;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.redis.DataStoreBackedCache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CacheService<K,V> {
    public enum Type {REDIS, GUAVA}

    protected ConcurrentMap<String, Cache<K,V>> caches = new ConcurrentHashMap<>();
    protected String name;
    private Type type;

    public CacheService(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    public <T extends Cache<K,V>> T getCache(String namespace) {
        return (T) caches.get(namespace);
    }

    public void registerCache(String namespace, Cache<K,V> cache) {
        caches.put(namespace, cache);
    }

    // TODO
    public void start() {

    }

    public void stop() {

    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }
}
