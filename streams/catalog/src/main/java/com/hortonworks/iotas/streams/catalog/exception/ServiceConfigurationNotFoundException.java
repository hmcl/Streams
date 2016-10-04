package com.hortonworks.iotas.streams.catalog.exception;

import com.hortonworks.iotas.streams.cluster.discovery.ambari.ComponentPropertyPattern;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.ServiceConfigurations;

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

    public ServiceConfigurationNotFoundException(Long clusterId, ServiceConfigurations service, String configurationName) {
        this(String.format("Configuration [%s] not found for service [%s] in cluster with id [%d]", configurationName, service.name(), clusterId));
    }
}
