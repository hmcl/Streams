package org.apache.streamline.cache.exception;

/**
 * Exception thrown if no value exists for a specific {@link org.apache.streamline.storage.StorableKey} key,
 * i.e. no key exists in storage.
 * */
public class NonexistentStorableKeyException extends RuntimeException {
    public NonexistentStorableKeyException(String message) {
        super(message);
    }
}
