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
import com.hortonworks.iotas.cache.view.config.Type;
import com.hortonworks.iotas.cache.view.impl.redis.RedisHashesCache;
import com.hortonworks.iotas.cache.view.impl.redis.RedisStringsCache;
import com.hortonworks.iotas.cache.view.impl.redis.connection.AbstractRedisConnectionFactory;
import com.hortonworks.iotas.cache.view.redis.DataStoreBackedCache;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisConnectionPool;
import com.lambdaworks.redis.codec.RedisCodec;

public class RedisCacheService<K,V> extends CacheService<K, V> {
    public static String REDIS_STRINGS_CACHE = "REDIS_STRINGS_CACHE";

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

    private void registerHashesCache(String id, K key) {
        if (isDataStoreBacked()) {
            caches.putIfAbsent(id, createDataStoreBackedRedisHashesCache(key));
        } else {
            caches.putIfAbsent(id, createRedisHashesCache(key));
        }
    }

    public void registerStringsCache() {
        if (isDataStoreBacked()) {
            caches.putIfAbsent(REDIS_STRINGS_CACHE, createDataStoreBackedRedisStringsCache());
        } else {
            caches.putIfAbsent(REDIS_STRINGS_CACHE, createRedisStringsCache());
        }
    }

    public void registerDelegateCache(String name) {
        //TODO
    }

    private RedisHashesCache<K, V> createRedisHashesCache(K key) {
        return new RedisHashesCache<>(connFactory.create(), key);
    }

    public Factory<RedisConnection<K, V>> getConnFactory() {
        return connFactory;
    }

    private DataStoreBackedCache<K, V> createDataStoreBackedRedisHashesCache(K key) {
        return new DataStoreBackedCache<>(createRedisHashesCache(key), getDataStore(), getCacheLoader(), getDataStoreWriter());
    }

    private RedisStringsCache<K, V> createRedisStringsCache() {
        return new RedisStringsCache<>(connFactory.create());
    }

    private DataStoreBackedCache<K, V> createDataStoreBackedRedisStringsCache() {
        return new DataStoreBackedCache<>(createRedisStringsCache(), getDataStore(), getCacheLoader(), getDataStoreWriter());
    }

}
