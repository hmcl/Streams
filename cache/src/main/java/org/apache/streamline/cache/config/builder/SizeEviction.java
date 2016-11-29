package org.apache.streamline.cache.config.builder;

import org.apache.streamline.cache.config.jackson.ExpiryPolicy;

class SizeEviction implements Eviction<ExpiryPolicy.Size> {
    @Override
    public ExpiryPolicy.Size eviction() {
        return null;
    }
}
