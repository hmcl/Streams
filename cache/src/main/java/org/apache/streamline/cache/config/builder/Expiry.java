package org.apache.streamline.cache.config.builder;

import org.apache.streamline.cache.config.jackson.ExpiryPolicy;

import java.util.function.Supplier;

interface Expiry extends Supplier<ExpiryPolicy.Ttl> {
    /*ExpiryPolicy.Ttl creation();

    ExpiryPolicy.Ttl access();

    ExpiryPolicy.Ttl update();*/
}
