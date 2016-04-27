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
import com.hortonworks.iotas.cache.view.config.CacheConfig;
import com.hortonworks.iotas.cache.view.config.ConnectionConfig;
import com.hortonworks.iotas.cache.view.config.DataStoreConfig;
import com.hortonworks.iotas.cache.view.config.ExpiryPolicy;
import com.hortonworks.iotas.cache.view.config.TypeConfig;
import com.hortonworks.iotas.cache.view.datastore.DataStoreReader;
import com.hortonworks.iotas.cache.view.impl.redis.connection.RedisConnectionFactory;
import com.hortonworks.iotas.cache.view.impl.redis.connection.RedisConnectionPoolFactory;
import com.hortonworks.iotas.cache.view.io.loader.CacheLoader;
import com.hortonworks.iotas.cache.view.io.loader.CacheLoaderSync;
import com.hortonworks.iotas.cache.view.io.writer.CacheWriter;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.codec.Utf8StringCodec;

import java.util.Arrays;
import java.util.Map;

public class RedisCacheServiceBuilder {
    private CacheConfig cacheConfig;
    private RedisCacheService redisCacheService;

    public RedisCacheServiceBuilder(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    private void buildCacheLevelConfig() {
        String id = cacheConfig.getId();
        TypeConfig.Cache cacheType = cacheConfig.getCacheType();
        redisCacheService = (RedisCacheService) new RedisCacheService.Builder<>(id, cacheType, getRedisConnectionFactory())
                .setCacheLoader(getCacheLoader())
                .setCacheWriter(getCacheWriter())
                .setDataStoreReader(getDataStoreReader())
                .setExpiryPolicy(getExpiryPolicy())
                .build();
    }

    private ExpiryPolicy getExpiryPolicy() {
        return null;
    }

    private void callCreate(String type) {
        switch (type) {
            case "a":
                String s = "a";
                Integer i = 1;
                create(String.class, Integer.class);
            case "b":
                break;
            default:
                break;
        }
    }

    private <K,V> void create(K k, V v) {
        RedisCacheService<Object, Object> build = new RedisCacheService.Builder<>(null, null, null).build();
    }

    private DataStoreReader<Object, Object> getDataStoreReader() {
        return null;
    }

    private CacheWriter<Object,Object> getCacheWriter() {
    }

    private CacheLoader<Object, Object> getCacheLoader() {
        DataStoreConfig dataStore = cacheConfig.getDataStore();
        if (dataStore != null) {
            final TypeConfig.CacheLoader cacheLoaderType = dataStore.getCacheLoaderType();
            switch (cacheLoaderType) {
                case SYNC:
                    return new CacheLoaderSync<>()
                    break;
                case ASYNC:
                    break;
                default:
                    throw new IllegalStateException("Invalid CacheLoader option. " + cacheLoaderType + ". Valid options are " + Arrays.toString(TypeConfig.CacheLoader.values()));
            }
        }
        dataStore.get
        return null;
    }

    private Factory<RedisConnection<Object, Object>> getRedisConnectionFactory() {
        ConnectionConfig.RedisConnectionConfig connectionConfig = (ConnectionConfig.RedisConnectionConfig) cacheConfig.getConnectionConfig();

        if (connectionConfig != null) {
            if (connectionConfig.getPool() != null) {
                //TODO only working for strings now
                return new RedisConnectionPoolFactory<>(RedisClient.create(getRedisUri()), getRedisCodec());
            } else {
                return new RedisConnectionFactory<>(RedisClient.create(getRedisUri()));
            }
        }
    }

    private RedisCodec<Object, Object> getRedisCodec() {
        //TODO
        return null;
    }

    private String getRedisUri() {
        ConnectionConfig.RedisConnectionConfig rcc = (ConnectionConfig.RedisConnectionConfig) cacheConfig.getConnectionConfig();
        return "redis://" +  rcc.getHost() + ":" + rcc.getPort();
    }

    class Registry {
        Map<String, Service> map;

        <K,V> Service<K,V> getService(String key) {
            return map.get(key);
        }
    }

    class Service<K,V> {
        K getKey() {
            return null;
        }

        V getVal() {
            return null;
        }
    }



}
