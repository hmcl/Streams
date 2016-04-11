package com.hortonworks.iotas.cache.redis.service;

import java.net.URI;

public class CacheServiceId {
    private URI uri;
    private final String id;

    public CacheServiceId(URI uri) {
        this(uri.toString());
        this.uri = uri;
    }

    public CacheServiceId(String id) {
        this.id = id;
    }

    public URI getUri() {
        return uri;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CacheServiceId that = (CacheServiceId) o;

        return !(id != null ? !id.equals(that.id) : that.id != null);
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
