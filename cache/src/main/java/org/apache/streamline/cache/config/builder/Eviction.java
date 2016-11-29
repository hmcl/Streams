package org.apache.streamline.cache.config.builder;

import org.apache.streamline.cache.config.jackson.ExpiryPolicy;

interface Eviction<T> {
    T eviction();

    ExpiryPolicy.Ttl ttl();

    boolean isTtl();

    ExpiryPolicy.Size size();

    boolean isSize();
}
