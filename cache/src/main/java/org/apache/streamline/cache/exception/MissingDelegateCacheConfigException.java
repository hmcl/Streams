package org.apache.streamline.cache.exception;

public class MissingDelegateCacheConfigException extends CacheException {

    public MissingDelegateCacheConfigException() {
        super();
    }

    public MissingDelegateCacheConfigException(Throwable cause) {
        super(cause);
    }

    public MissingDelegateCacheConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public MissingDelegateCacheConfigException(String message) {
        super(message);
    }
}
