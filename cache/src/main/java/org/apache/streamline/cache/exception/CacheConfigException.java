package org.apache.streamline.cache.exception;

//TODO: Should this the a Checked Exception instead of a RuntimeException
public class CacheConfigException extends RuntimeException {

    public CacheConfigException() {
        super();
    }

    public CacheConfigException(Throwable cause) {
        super(cause);
    }

    public CacheConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public CacheConfigException(String message) {
        super(message);
    }
}
