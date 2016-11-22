package org.apache.streamline.cache;

public interface CacheFactory<T> {
    T create();
}
