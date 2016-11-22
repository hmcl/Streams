package org.apache.streamline.cache.services;

public interface Service extends AutoCloseable {
    void init();

    @Override
    void close() throws Exception;
}
