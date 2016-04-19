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

import com.hortonworks.iotas.cache.redis.RedisStringsCache;
import com.hortonworks.iotas.cache.redis.datastore.DataStore;
import com.hortonworks.iotas.cache.redis.datastore.writer.DataStoreWriter;
import com.hortonworks.iotas.cache.redis.loader.CacheLoader;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisConnectionPool;
import com.lambdaworks.redis.codec.RedisCodec;

public class RedisCacheService<K,V> extends CacheService<K, V> {
    public static String REDIS_STRINGS_CACHE = "REDIS_STRINGS_CACHE";

    private RedisConnectionFactory connectionFactory;

    public RedisCacheService(RedisConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public static class Builder<K,V> {
        private RedisClient redisClient;
        private final CacheLoader<K, V> cacheLoader;
        private final DataStoreWriter<K, V> dataStoreWriter;
        private final DataStore<K, V> dataStore;

        public Builder(RedisClient redisClient) {
            this.redisClient = redisClient;
        }




    }

    public static class Builder {
        public Builder(RedisClient redisClient) {
            this.redisClient = redisClient;
        }
    }

    private void registerHashesCache(String name) {


    }


    public void registerStringsCache() {
        caches.putIfAbsent(REDIS_STRINGS_CACHE, new RedisStringsCache<K, V>(redisConnection));
    }


    public class RedisConnectionFactory  {
        // Defaults for Lettuce Redis Client 3.4.2
        private static final int MAX_IDLE = 5;
        private static final int MAX_ACTIVE = 20;

        private RedisClient redisClient;

        public RedisConnectionFactory(RedisClient redisClient) {
            this.redisClient = redisClient;
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
