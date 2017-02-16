package org.apache.streamline.cache.services;

public interface CacheService extends AutoCloseable {
    void init();

    @Override
    void close() throws Exception;
}
