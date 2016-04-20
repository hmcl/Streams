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

import java.util.concurrent.TimeUnit;

public class RedisCacheService<K,V> extends CacheService<K, V> {
    public static String REDIS_STRINGS_CACHE = "REDIS_STRINGS_CACHE";

    private RedisClient redisClient;
    private CacheLoader<K, V> cacheLoader;
    private DataStoreWriter<K, V> dataStoreWriter;
    private DataStore<K, V> dataStore;

    private RedisConnectionFactory connectionFactory;

    public RedisCacheService(Builder builder) {
        super(builder.name, builder.type);
        this.redisClient = builder.redisClient;
        this.cacheLoader = builder.cacheLoader;
        this.dataStoreWriter = builder.dataStoreWriter;
        this.dataStore = builder.dataStore;
    }

    public class CacheConfig {
        String name;
        Type type;
        ConnectionConfig connectionConfig;
        DataStoreConfig dataStoreConfig;


    }

    public class ConnectionConfig {
        private String host;
        private String port;

        public ConnectionConfig(String host, String port) {
            this.host = host;
            this.port = port;
        }
    }

    public class RedisConnectionConfig extends ConnectionConfig{
        private Pool pool;

        class Pool {
            int min;
            int max;

            public Pool(int min, int max) {
                this.min = min;
                this.max = max;
            }
        }

        public RedisConnectionConfig(String host, String port, Pool pool) {
            super(host, port);
            this.pool = pool;
        }
    }



    enum DataStoreType {PHOENIX, MYSQL, H_BASE}
    enum CacheLoadingType{SYNC, ASYNC}
    enum ReadingType{THROUGH}
    enum WritingType{SYNC, ASYNC}


    public class DataStoreConfig {
        String name;
        DataStoreType type;
        ConnectionConfig config;
        CacheLoadingType loadingType;
        ReadingType readingType;
        WritingType writingType;
    }

    public class ExpiryPolicy {
        class Ttl {
            long count;
            TimeUnit unit;

            public Ttl(long count, TimeUnit unit) {
                this.count = count;
                this.unit = unit;
            }
        }

        class Size {
            long count;
            BytesUnit unit;

            public Size(long count, BytesUnit unit) {
                this.count = count;
                this.unit = unit;
            }
        }

        private Ttl ttl;
        private long entries;
        private Size size;

        public ExpiryPolicy(Ttl ttl, long entries, Size size) {
            this.ttl = ttl;
            this.entries = entries;
            this.size = size;
        }
    }

    public enum BytesUnit {
        BYTES {
        public long toBytes(long d) { return d; }
        public long toKilobytes(long d) { return d/RATIO; }
        public long toMegabytes(long d)  { return d/RATIO_POW_2; }
        },
        KILOBYTES {
            public long toBytes(long d)  { return d*RATIO; }
            public long toKilobytes(long d) { return d; }
            public long toMegabytes(long d)  { return d/RATIO; }
        },
        MEGABYTES {
            public long toBytes(long d)  { return d*RATIO_POW_2; }
            public long toKilobytes(long d) { return d*RATIO; }
            public long toMegabytes(long d)  { return d; }
        };

        private static final long RATIO = 1024;
        private static final long RATIO_POW_2 = 1024*1024;

        public long toBytes(long d)  {throw new AbstractMethodError(); }
        public long toKilobytes(long d) {throw new AbstractMethodError(); }
        public long toMegabytes(long d) {throw new AbstractMethodError(); }
    }




    public static class Builder<K,V> {
        private final RedisClient redisClient;
        private final String name;
        private final Type type;
        private CacheLoader<K, V> cacheLoader;
        private DataStoreWriter<K, V> dataStoreWriter;
        private DataStore<K, V> dataStore;

        public Builder(RedisClient redisClient, String name, Type type) {
            this.redisClient = redisClient;
            this.name = name;
            this.type = type;
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
