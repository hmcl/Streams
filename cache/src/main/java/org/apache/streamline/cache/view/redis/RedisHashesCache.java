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

package org.apache.streamline.cache.view.redis;

import com.lambdaworks.redis.RedisConnection;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.config.jackson.ExpiryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unchecked")
public class RedisHashesCache<K, V> extends RedisAbstractCache<K, V> implements Cache<K, V> {
    private static   final Logger LOG = LoggerFactory.getLogger(RedisHashesCache.class);

    private final K key;    // The key of the Redis Hashes Cache

    public RedisHashesCache(RedisConnection<K, V> redisConnection, K key) {
        this(redisConnection, key, null);
    }

    public RedisHashesCache(RedisConnection<K, V> redisConnection, K key, ExpiryPolicy expiryPolicy) {
        super(redisConnection, expiryPolicy);
        this.key = key;
    }

    @Override
    public V get(K field) {
        return redisConnection.hget(key, field);
    }

    @Override
    public Map<K, V> getAll(Collection<? extends K> fields) {
        // all key/val pairs for the fields specified. If a key does not exist in Redis, a null value will be returned for that key
        final List<V> all = redisConnection.hmget(key, toArray(fields));
        final Map<K,V> inCache = Collections.emptyMap();    // val for a given field is not null
        final Set<K> notInCache = new HashSet<>(fields);    // val for a given field is null

        if(all != null) {
            int i = 0;
            for (K field : fields) {
                if (i < all.size()) {       //Check just to be safe
                    V val = all.get(i);
                    if (val != null) {      // key present in cache
                        inCache.put(field, val);
                    } else {
                        notInCache.add(field);
                    }
                }
                i++;
            }
        }
        LOG.debug("Entries in cache [{}]. Keys not in cache: [{}]", inCache, notInCache);
        return inCache;
    }

    private K[] toArray(Collection<? extends K> fields) {
        return fields.toArray(((K[]) new Object[fields.size()]));
    }

    @Override
    public void put(K field, V val) {
        redisConnection.hset(key, field, val);
        LOG.debug("Set (key, field, val) => ({},{})", key, field, val);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        redisConnection.hmset(key, new HashMap<>(entries));
    }

    @Override
    public void remove(K field) {
        redisConnection.hdel(key, field);
    }

    @Override
    public void removeAll(Collection<? extends K> fields) {
        redisConnection.hdel(key, fields.toArray(((K[]) new Object[fields.size()])));
    }

    @Override
    public void clear() {
        redisConnection.del(key);
    }

    @Override
    public long size() {
        return redisConnection.hlen(key);
    }

    @Override
    public <S> S stats() {
        return null;
    }
}
