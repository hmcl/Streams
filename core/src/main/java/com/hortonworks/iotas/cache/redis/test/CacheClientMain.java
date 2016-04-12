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

package com.hortonworks.iotas.cache.redis.test;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.redis.datastore.DataStore;
import com.hortonworks.iotas.cache.redis.service.CacheService;
import com.hortonworks.iotas.cache.redis.service.CacheServiceFactory;
import com.hortonworks.iotas.cache.redis.service.CacheServiceId;
import com.hortonworks.iotas.cache.redis.service.registry.CacheServiceRegistry;

import org.apache.commons.collections.map.HashedMap;

import java.util.HashMap;

public class CacheClientMain {
    private static final CacheServiceRegistry cacheRegistry = CacheServiceRegistry.INSTANCE;
    public static void main(String[] args) {

    }

    public void method() {
        CacheService<String, String> cacheService = cacheRegistry.getCacheService(CacheServiceId.redis("localhost", 6379));
        Cache<String, String> cache = cacheService.getCache();
        String val = cache.get("key");
        cache.put("key", "val");
        cache.putAll(new HashMap<String, String>(){{put("key", "val"); put("key1", "val1");}});
    }

    public void registerCache() {
        cacheRegistry.register(CacheServiceId.redis("localhost", 6379), new CacheService<>(new CacheServiceFactory<String, String>() {
            @Override
            public Cache createCache() {
                return null;
            }

            @Override
            public DataStore createDataStore() {
                return null;
            }
        }));
        Cache<String, String> cache = cacheService.getCache();
        String val = cache.get("key");
        cache.put("key", "val");
        cache.putAll(new HashMap<String, String>(){{put("key", "val"); put("key1", "val1");}});
    }
}
