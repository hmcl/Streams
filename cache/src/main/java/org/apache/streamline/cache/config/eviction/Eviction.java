package org.apache.streamline.cache.config.eviction;

public interface Eviction {
    Long entries();

    boolean isEntries();

    Size size();

    boolean isSize();
}
