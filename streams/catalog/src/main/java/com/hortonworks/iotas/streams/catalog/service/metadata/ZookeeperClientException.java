package com.hortonworks.iotas.streams.catalog.service.metadata;


/**
 * Wraps Curator Framework exceptions. It is useful to do this because several Curator Framework methods throw the generic
 * {@link Exception}, which makes it impossible to handle more specific exceptions in code that calls these methods.
 */
public class ZookeeperClientException extends Exception {
    public ZookeeperClientException() {
        super();
    }

    public ZookeeperClientException(String message) {
        super(message);
    }

    public ZookeeperClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public ZookeeperClientException(Throwable cause) {
        super(cause);
    }

    protected ZookeeperClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
