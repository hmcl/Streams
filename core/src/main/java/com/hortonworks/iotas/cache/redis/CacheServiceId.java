package com.hortonworks.iotas.cache.redis;

import java.net.URI;

public class CacheServiceId {
    private final URI uri;
    private final String alias;    // alias

    public CacheServiceId(URI uri, String alias) {
        this.uri = uri;
        this.alias = alias;
    }

    public CacheServiceId(URI uri) {
        this(uri, uri.toString());
    }

    public URI getUri() {
        return uri;
    }

    public String getAlias() {
        return alias;
    }
}
