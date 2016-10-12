package com.hortonworks.iotas.streams.catalog.exception;

import com.hortonworks.iotas.streams.cluster.discovery.ambari.ComponentPropertyPattern;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.ServiceConfigurations;

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

    public ServiceComponentNotFoundException(Long clusterId, ServiceConfigurations service, ComponentPropertyPattern component) {
        this(String.format("Component [%s] not found for service [%s] in cluster with id [%d]", component.name(), service.name(), clusterId));
    }
}
