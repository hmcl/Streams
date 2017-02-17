package org.apache.streamline.cache.exception;

public class CacheNotFoundException extends CacheException {

    public CacheNotFoundException() {
        super();
    }

    public CacheNotFoundException(String message) {
        super(message);
    }

    public CacheNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public CacheNotFoundException(Throwable cause) {
        super(cause);
    }
}
