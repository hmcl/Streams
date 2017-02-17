package org.apache.streamline.cache.exception;

public class CacheServiceException extends CacheException {

    public CacheServiceException() {
        super();
    }

    public CacheServiceException(Throwable cause) {
        super(cause);
    }

    public CacheServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public CacheServiceException(String message) {
        super(message);
    }
}
