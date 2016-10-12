package com.hortonworks.iotas.streams.catalog.exception;

public class ServiceComponentNotFoundException extends EntityNotFoundException {
    public ServiceComponentNotFoundException() {
        super();
    }

    public ServiceComponentNotFoundException(String message) {
        super(message);
    }

    public ServiceComponentNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public ServiceComponentNotFoundException(Throwable cause) {
        super(cause);
    }

    protected ServiceComponentNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
