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

import com.hortonworks.iotas.cache.redis.RedisHashesCache;
import com.hortonworks.iotas.cache.redis.RedisStringsCache;
import com.hortonworks.iotas.cache.redis.config.Type;
import com.hortonworks.iotas.cache.redis.datastore.DataStore;
import com.hortonworks.iotas.cache.redis.datastore.writer.DataStoreWriter;
import com.hortonworks.iotas.cache.redis.loader.CacheLoader;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisConnectionPool;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.codec.Utf8StringCodec;

public class RedisCacheService<K,V> extends CacheService<K, V> {
    public static String REDIS_STRINGS_CACHE = "REDIS_STRINGS_CACHE";

    private RedisClient redisClient;
    private CacheLoader<K, V> cacheLoader;
    private DataStoreWriter<K, V> dataStoreWriter;
    private DataStore<K, V> dataStore;
    private RedisConnectionFactory redisConnectionFactory;

    private RedisConnectionFactory connectionFactory;

    public RedisCacheService(Builder builder) {
        super(builder.name, builder.type);
        this.redisClient = builder.redisClient;
        this.cacheLoader = builder.cacheLoader;
        this.dataStoreWriter = builder.dataStoreWriter;
        this.dataStore = builder.dataStore;
    }


    public static class Builder<K,V> {
        private final RedisClient redisClient;
        private final String name;
        private final Type.Cache cacheType;
        private CacheLoader<K, V> cacheLoader;
        private DataStoreWriter<K, V> dataStoreWriter;
        private DataStore<K, V> dataStore;

        public Builder(RedisClient redisClient, String name, Type.Cache cacheType) {
            this.redisClient = redisClient;
            this.name = name;
            this.cacheType= cacheType;
        }

        public Builder setCacheLoader(CacheLoader<K, V> cacheLoader) {
            this.cacheLoader = cacheLoader;
            return this;
        }

        public Builder setDataStoreWriter(DataStoreWriter<K, V> dataStoreWriter) {
            this.dataStoreWriter = dataStoreWriter;
            return this;
        }

        public Builder setDataStore(DataStore<K, V> dataStore) {
            this.dataStore = dataStore;
            return this;
        }

        public RedisCacheService build() {
            return new RedisCacheService(this);
        }
    }

    private void registerHashesCache(String key) {
        caches.putIfAbsent(key, new RedisHashesCache<K, V>(redisConnectionFactory.createConnection()))
    }


    public void registerStringsCache() {
        caches.putIfAbsent(REDIS_STRINGS_CACHE, new RedisStringsCache<K, V>(redisConnectionFactory.createConnection()));
    }

    public void registerDelegateCache(String name) {
        caches.putIfAbsent(name, new RedisStringsCache<K, V>(redisConnectionFactory.createConnection()));
    }


    public class RedisConnectionFactory<K,V>  {
        // Defaults for Lettuce Redis Client 3.4.2
        private static final int MAX_IDLE = 5;
        private static final int MAX_ACTIVE = 20;

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
