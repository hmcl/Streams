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

import com.hortonworks.iotas.cache.view.Factory;
import com.hortonworks.iotas.cache.view.config.ExpiryPolicy;
import com.hortonworks.iotas.cache.view.config.Type;
import com.hortonworks.iotas.cache.view.impl.redis.RedisHashesCache;
import com.hortonworks.iotas.cache.view.impl.redis.RedisStringsCache;
import com.hortonworks.iotas.cache.view.DataStoreBackedCache;
import com.lambdaworks.redis.RedisConnection;

public class RedisCacheService<K,V> extends CacheService<K, V> {
    private Factory<RedisConnection<K,V>> connFactory;


    private RedisCacheService(Builder<K,V> builder) {
        super(builder);
        this.connFactory = builder.connFactory;
    }


    public static class Builder<K,V> extends CacheService.Builder<K,V> {
        private Factory<RedisConnection<K,V>> connFactory;

        public Builder(String id, Type.Cache cacheType, Factory<RedisConnection<K,V>> connFactory) {
            super(id, cacheType);
            this.connFactory = connFactory;
        }

        public RedisCacheService<K,V> build() {
            return new RedisCacheService<>(this);
        }
    }

    public void registerHashesCache(String id, K key) {
        registerHashesCache(id, key, expiryPolicy);
    }

    public void registerHashesCache(String id, K key, ExpiryPolicy expiryPolicy) {
        if (isDataStoreBacked()) {
            caches.putIfAbsent(id, createDataStoreBackedRedisHashesCache(key, expiryPolicy));
        } else {
            caches.putIfAbsent(id, createRedisHashesCache(key, expiryPolicy));
        }
    }

    public void registerStringsCache(String id) {
        registerStringsCache(id, expiryPolicy);
    }

    public void registerStringsCache(String id, ExpiryPolicy expiryPolicy) {
        if (isDataStoreBacked()) {
            caches.putIfAbsent(id, createDataStoreBackedRedisStringsCache(expiryPolicy));
        } else {
            caches.putIfAbsent(id, createRedisStringsCache(expiryPolicy));
        }
    }

    public void registerDelegateCache(String id) {
        //TODO
    }

    public Factory<RedisConnection<K, V>> getConnFactory() {
        return connFactory;
    }

    private RedisHashesCache<K, V> createRedisHashesCache(K key, ExpiryPolicy expiryPolicy) {
        return new RedisHashesCache<>(connFactory.create(), key, expiryPolicy);
    }

    private DataStoreBackedCache<K, V> createDataStoreBackedRedisHashesCache(K key, ExpiryPolicy expiryPolicy) {
        return new DataStoreBackedCache<>(createRedisHashesCache(key, expiryPolicy), dataStore, cacheLoader, dataStoreWriter);
    }

    private RedisStringsCache<K, V> createRedisStringsCache(ExpiryPolicy expiryPolicy) {
        return new RedisStringsCache<>(connFactory.create(), expiryPolicy);
    }

    private DataStoreBackedCache<K, V> createDataStoreBackedRedisStringsCache(ExpiryPolicy expiryPolicy) {
        return new DataStoreBackedCache<>(createRedisStringsCache(expiryPolicy), dataStore, cacheLoader, dataStoreWriter);
    }
}
