package com.hortonworks.iotas.streams.catalog.exception;

public class ServiceConfigurationNotFoundException extends EntityNotFoundException {
    public ServiceConfigurationNotFoundException() {
        super();
    }

    public ServiceConfigurationNotFoundException(String message) {
        super(message);
    }

    public ServiceConfigurationNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public ServiceConfigurationNotFoundException(Throwable cause) {
        super(cause);
    }

    protected ServiceConfigurationNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
