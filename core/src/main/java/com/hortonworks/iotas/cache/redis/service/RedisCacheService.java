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

import com.hortonworks.iotas.cache.redis.DataStoreBackedCache;
import com.hortonworks.iotas.cache.redis.RedisHashesCache;
import com.hortonworks.iotas.cache.redis.RedisStringsCache;
import com.hortonworks.iotas.cache.redis.config.Type;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisConnectionPool;
import com.lambdaworks.redis.codec.RedisCodec;

public class RedisCacheService<K,V> extends CacheService<K, V> {
    public static String REDIS_STRINGS_CACHE = "REDIS_STRINGS_CACHE";

    private RedisClient redisClient;

    private Factory<RedisConnection<K,V>> connFactory;


    private RedisCacheService(Builder builder) {
        super(builder);
        this.redisClient = builder.redisClient;
    }


    public static class Builder<K,V> extends CacheService.Builder<K,V> {
        private final RedisClient redisClient;

        public Builder(String id, Type.Cache cacheType, RedisClient redisClient, Factory<RedisConnection<K,V>> factory) {
            super(id, cacheType);
            this.redisClient = redisClient;
        }

        public RedisCacheService build() {
            return new RedisCacheService(this);
        }
    }

    private void registerHashesCache(String id, K key) {
        if (isDataStoreBacked()) {
            caches.putIfAbsent(id, createDataStoreBackedRedisHashesCache(key));
        } else {
            caches.putIfAbsent(id, createRedisHashesCache(key));
        }
    }

    private RedisHashesCache<K, V> createRedisHashesCache(K key) {
        return new RedisHashesCache<>(connFactory.create(), key);
    }

    private DataStoreBackedCache<K, V> createDataStoreBackedRedisHashesCache(K key) {
        return new DataStoreBackedCache<>(createRedisHashesCache(key), getCacheLoader(), getDataStoreWriter());
    }


    public void registerStringsCache() {
        if (isDataStoreBacked()) {
            caches.putIfAbsent(REDIS_STRINGS_CACHE, createDataStoreBackedRedisStringsCache());
        } else {
            caches.putIfAbsent(REDIS_STRINGS_CACHE, createRedisStringsCache());
        }
    }

    private RedisStringsCache<K, V> createRedisStringsCache() {
        return new RedisStringsCache<>(connFactory.create());
    }

    private DataStoreBackedCache<K, V> createDataStoreBackedRedisStringsCache() {
        return new DataStoreBackedCache<>(createRedisStringsCache(), getCacheLoader(), getDataStoreWriter());
    }


    public void registerDelegateCache(String name) {
        caches.putIfAbsent(name, new RedisStringsCache<K, V>(connFactory.create()));
    }


    public interface Factory<T> {
        T create();
    }

    public abstract class AbstractRedisConnectionFactory<K,V,T> implements Factory<T> {
        protected RedisClient redisClient;
        protected RedisCodec<K, V> codec;

        public AbstractRedisConnectionFactory(RedisClient redisClient, RedisCodec<K, V> codec) {
            this.redisClient = redisClient;
            this.codec = codec;
        }

        public RedisClient getRedisClient() {
            return redisClient;
        }

        public RedisCodec<K, V> getCodec() {
            return codec;
        }
    }

    public class RedisConnectionFactory<K,V> extends AbstractRedisConnectionFactory<K,V, RedisConnection<K, V> > {
        public RedisConnectionFactory(RedisClient redisClient, RedisCodec<K, V> codec) {
            super(redisClient, codec);
        }

        @Override
        public RedisConnection<K, V> create() {
            return redisClient.connect(codec);
        }
    }

    public class RedisConnectionPoolFactory<K,V> extends AbstractRedisConnectionFactory<K,V, RedisConnectionPool<RedisConnection<K, V>>> {
        // Defaults for Lettuce Redis Client 3.4.2
        private static final int MAX_IDLE = 5;
        private static final int MAX_ACTIVE = 20;

        public RedisConnectionPoolFactory(RedisClient redisClient, RedisCodec<K, V> codec) {
            super(redisClient, codec);
        }

        public RedisConnectionPool<RedisConnection<K, V>> create() {
            return redisClient.pool(codec, MAX_IDLE, MAX_ACTIVE);
        }
    }
}
