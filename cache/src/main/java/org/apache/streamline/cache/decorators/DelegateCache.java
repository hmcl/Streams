package org.apache.streamline.cache.decorators;

import org.apache.streamline.cache.Cache;

import java.util.Collection;
import java.util.Map;

public class DelegateCache<K, V> implements Cache<K, V> {
    protected final Cache<K,V> delegate;

    public DelegateCache(Cache<K, V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public V get(K key) {
        return delegate.get(key);
    }

    @Override
    public Map<K, V> getAll(Collection<? extends K> keys) {
        return delegate.getAll(keys);
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
        delegate.remove(key);
    }

    @Override
    public void removeAll(Collection<? extends K> keys) {
        delegate.removeAll(keys);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public <S> S stats() {
        return delegate.stats();
    }
}
