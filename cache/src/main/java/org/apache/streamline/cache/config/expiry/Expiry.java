package org.apache.streamline.cache.config.expiry;

public interface Expiry {
    Ttl onCreation();

    boolean isOnCreation();

    Ttl onAccess();

    boolean isOnAccess();

    Ttl onUpdate();

    boolean isOnUpdate();
}

