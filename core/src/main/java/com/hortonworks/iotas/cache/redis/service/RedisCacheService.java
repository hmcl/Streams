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

    private RedisConnectionFactory redisConnectionFactory;

    private RedisConnectionFactory connectionFactory;

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

    private void registerHashesCache(String key) {
        caches.putIfAbsent(key, createRedisHashesCash());
    }

    private RedisHashesCache<K, V> createRedisHashesCash() {
        return new RedisHashesCache<K, V>(redisConnectionFactory.createConnection());
    }

    private RedisHashesCache<K, V> createDataStoreBackedRedisHashesCash() {
        return new DataStoreBackedCache<>(createRedisHashesCash());
    }


    public void registerStringsCache() {
        caches.putIfAbsent(REDIS_STRINGS_CACHE, new RedisStringsCache<K, V>(redisConnectionFactory.createConnection()));
    }

    public void registerDelegateCache(String name) {
        caches.putIfAbsent(name, new RedisStringsCache<K, V>(redisConnectionFactory.createConnection()));
    }


    public interface IRedisConnectionFactory<K,V>  {
        public RedisConnection<K, V> create();
    }

    public interface Factory<T> {
        T create();
    }


    public class RedisConnectionFactory<K,V> implements Factory<RedisConnection<K,V>> {
        // Defaults for Lettuce Redis Client 3.4.2
        private static final int MAX_IDLE = 5;
        private static final int MAX_ACTIVE = 20;

        @Override
        public RedisConnection<K, V> create() {
            return null;
        }



        private RedisClient redisClient;
        private RedisCodec<K, V> codec;


        public RedisConnectionFactory(RedisClient redisClient, RedisCodec<K, V> codec) {
            this.redisClient = redisClient;
            this.codec = codec;
        }

        public RedisConnection<String, String> createStringsConnection() {
            return redisClient.connect();
        }

        public RedisConnection<K, V> createConnection(RedisCodec<K, V> codec) {
            return redisClient.connect(codec);
        }

        public RedisConnectionPool<RedisConnection<String, String>> createStringsConnectionPool() {
            return redisClient.pool();
        }

        public RedisConnectionPool<RedisConnection<K, V>> createConnectionPool(RedisCodec<K, V> codec) {
            return redisClient.pool(codec, MAX_IDLE, MAX_ACTIVE);
        }

        public RedisClient getRedisClient() {
            return redisClient;
        }
    }


}
