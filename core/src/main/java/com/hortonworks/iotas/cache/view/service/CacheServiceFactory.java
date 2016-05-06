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
import com.hortonworks.iotas.cache.view.datastore.DataStoreWriter;
import com.hortonworks.iotas.cache.view.datastore.phoenix.PhoenixDataStore;
import com.hortonworks.iotas.cache.view.impl.redis.connection.AbstractRedisConnectionFactory;
import com.hortonworks.iotas.cache.view.impl.redis.connection.RedisConnectionFactory;
import com.hortonworks.iotas.cache.view.impl.redis.connection.RedisConnectionPoolFactory;
import com.hortonworks.iotas.cache.view.io.loader.CacheLoaderAsyncFactory;
import com.hortonworks.iotas.cache.view.io.loader.CacheLoaderFactory;
import com.hortonworks.iotas.cache.view.io.loader.CacheLoaderSyncFactory;
import com.hortonworks.iotas.cache.view.io.writer.CacheWriter;
import com.hortonworks.iotas.cache.view.io.writer.CacheWriterAsync;
import com.hortonworks.iotas.cache.view.io.writer.CacheWriterSync;
import com.hortonworks.iotas.cache.view.service.registry.CacheServiceLocalRegistry;
import com.hortonworks.iotas.exception.InvalidCacheViewConfigException;
import com.hortonworks.iotas.exception.MissingCacheViewConfigException;
import com.hortonworks.iotas.util.ReflectionHelper;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.codec.Utf8StringCodec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class CacheServiceFactory<T extends CacheService> implements Factory<T>{
    private static final Logger LOG = LoggerFactory.getLogger(CacheServiceFactory.class);

    private CacheConfig cacheConfig;

    public CacheServiceFactory(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    public void register() {
        CacheServiceLocalRegistry.INSTANCE.register(new CacheServiceId(cacheConfig.getId()), create());
    }

    public T create() {
        TypeConfig.Cache cacheType = cacheConfig.getCacheType();
        switch (cacheType) {
            case REDIS:
                return (T) createRedisCacheService();
            case GUAVA:
                return (T) createGuavaCacheService();
            default:
                throw new UnsupportedOperationException("Invalid cache option. " + cacheType
                        + ". Valid options are " + Arrays.toString(TypeConfig.Cache.values()));
        }
    }

    private RedisCacheService createRedisCacheService() {
        final String cacheServiceId = cacheConfig.getId();
        final TypeConfig.Cache cacheType = cacheConfig.getCacheType();
        return (RedisCacheService) new RedisCacheService.Builder(cacheServiceId, cacheType, createRedisConnectionFactory())
                .setCacheLoaderFactory(createCacheLoaderFactory())
                .setCacheWriter(createCacheWriter(createDataStoreWriter(getNamespace())))
                .setDataStoreReader(createDataStoreReader(getNamespace()))
                .setExpiryPolicy(getExpiryPolicy())
                .build();
    }

    private CacheService createGuavaCacheService() {
        throw new UnsupportedOperationException("Must implement Guava Cache Service");
    }

    private ExpiryPolicy getExpiryPolicy() {
        return cacheConfig.getExpiryPolicy();
    }

    private String getNamespace() {
        return cacheConfig.getDataStore().getNamespace();
    }

    private DataStoreWriter createDataStoreWriter(String namespace) {
        final TypeConfig.DataStore dataStoreType = cacheConfig.getDataStore().getDataStoreType();
        switch (dataStoreType) {
            case PHOENIX:
                return new PhoenixDataStore<>(namespace);
            case MYSQL:
                throw new UnsupportedOperationException("MySQL DataStore not supported");
            case HBASE:
                throw new UnsupportedOperationException("HBASE DataStore not supported");
            default:
                throw new InvalidCacheViewConfigException("Invalid DataStore option. " + dataStoreType
                        + ". Valid options are " + Arrays.toString(TypeConfig.DataStore.values()));
        }
    }

    private DataStoreReader<Object, Object> createDataStoreReader(String namespace) {
        final TypeConfig.DataStore dataStoreType = cacheConfig.getDataStore().getDataStoreType();
        switch (dataStoreType) {
            case PHOENIX:
                return new PhoenixDataStore<>(namespace);
            case MYSQL:
                throw new UnsupportedOperationException("MySQL DataStore not supported");
            case HBASE:
                throw new UnsupportedOperationException("HBASE DataStore not supported");
            default:
                throw new InvalidCacheViewConfigException("Invalid DataStore option. " + dataStoreType
                        + ". Valid options are " + Arrays.toString(TypeConfig.DataStore.values()));
        }
    }

    private CacheWriter createCacheWriter(DataStoreWriter dataStoreWriter) {
        final TypeConfig.CacheWriter cacheWriterType = cacheConfig.getDataStore().getCacheWriterType();
        switch (cacheWriterType) {
            case SYNC:
                return new CacheWriterSync(dataStoreWriter);
            case ASYNC:
                return new CacheWriterAsync(dataStoreWriter);
            default:
                throw new InvalidCacheViewConfigException("Invalid CacheWriter option. " + cacheWriterType
                        + ". Valid options are " + Arrays.toString(TypeConfig.CacheWriter.values()));
        }
    }

    private CacheLoaderFactory createCacheLoaderFactory() {
        final DataStoreConfig dataStore = cacheConfig.getDataStore();
        if (dataStore != null) {
            final TypeConfig.CacheLoader cacheLoaderType = dataStore.getCacheLoaderType();
            switch (cacheLoaderType) {
                case SYNC:
                    return new CacheLoaderSyncFactory<>();
                case ASYNC:
                    return new CacheLoaderAsyncFactory<>();
                default:
                    throw new InvalidCacheViewConfigException("Invalid CacheLoader option. " + cacheLoaderType
                            + ". Valid options are " + Arrays.toString(TypeConfig.CacheLoader.values()));
            }
        }
        return null;
    }

    private Factory<RedisConnection> createRedisConnectionFactory() {
        final ConnectionConfig.RedisConnectionConfig redisConnectionConfig = (ConnectionConfig.RedisConnectionConfig) cacheConfig.getConnectionConfig();

        if (redisConnectionConfig != null) {
            final RedisURI redisURI = AbstractRedisConnectionFactory.createRedisUri(redisConnectionConfig);
            final ConnectionConfig.RedisConnectionConfig.Pool pool = redisConnectionConfig.getPool();
            if (pool != null) {
                return new RedisConnectionPoolFactory(redisURI, createRedisCodec(), pool.getMaxIdle(), pool.getMaxActive());
            } else {
                return new RedisConnectionFactory(redisURI, createRedisCodec());
            }
        } else {
            throw new MissingCacheViewConfigException("Must specify host and port configuration for Redis connection");
        }
    }

    private RedisCodec createRedisCodec() {
        final String codec = cacheConfig.getCacheEntry().getCodec();
        if (codec == null) {
            return new Utf8StringCodec();   // By default uses Strings Codec - TODO: Is this good ?
        } else {
            try {
                return ReflectionHelper.newInstance(codec);
            } catch (ClassCastException e) {
                throw new InvalidCacheViewConfigException("Codec must be the fqn of of a class that implements "
                        +  RedisCodec.class.getName(), e);
            }catch (Exception e) {
                throw new InvalidCacheViewConfigException("Exception occurred while creating codec", e);
            }
        }
    }
}
