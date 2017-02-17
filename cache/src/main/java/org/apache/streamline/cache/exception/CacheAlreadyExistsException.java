package org.apache.streamline.cache.exception;

public class CacheAlreadyExistsException extends CacheException {

    public CacheAlreadyExistsException() {
        super();
    }

    public CacheAlreadyExistsException(String message) {
        super(message);
    }

    public CacheAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public CacheAlreadyExistsException(Throwable cause) {
        super(cause);
    }
}
