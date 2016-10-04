package com.hortonworks.iotas.streams.catalog.exception;

public class MissingServiceConfigurationException extends Exception {
    public MissingServiceConfigurationException() {
        super();
    }

    public MissingServiceConfigurationException(String message) {
        super(message);
    }

    public MissingServiceConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public MissingServiceConfigurationException(Throwable cause) {
        super(cause);
    }

    protected MissingServiceConfigurationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
