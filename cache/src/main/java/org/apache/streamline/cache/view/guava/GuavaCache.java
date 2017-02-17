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

package org.apache.streamline.cache.view.guava;

import com.google.common.cache.CacheStats;

import org.apache.streamline.cache.Cache;
import org.apache.streamline.cache.config.jackson.ExpiryPolicy;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class GuavaCache<K,V> implements Cache<K,V> {
    private com.google.common.cache.Cache<K,V> delegate;

    public GuavaCache(com.google.common.cache.Cache<K, V> delegate) {
        Objects.requireNonNull(delegate, "Must specify an implementation of "
                + com.google.common.cache.Cache.class.getName());
        this.delegate = delegate;
    }

    @Override
    public V get(K key) {
        return delegate.getIfPresent(key);
    }

    @Override
    public Map<K, V> getAll(Collection<? extends K> keys) {
        return delegate.getAllPresent(keys);
    }

    @Override
    public void put(K key, V val) {
        delegate.put(key, val);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        delegate.putAll(entries);
    }

    @Override
    public void remove(K key) {
        delegate.invalidate(key);
    }

    @Override
    public void removeAll(Collection<? extends K> keys) {
        delegate.invalidateAll(keys);
    }

    @Override
    public void clear() {
        delegate.invalidateAll();
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @SuppressWarnings("unchecked")
    @Override
    public CacheStats stats() {
        return delegate.stats();
    }

    @Override
    public ExpiryPolicy getExpiryPolicy() {
        return null;
    }

    @Override
    public String toString() {
        return "GuavaCache{" +
                "delegate=" + delegate +
                '}';
    }
}
