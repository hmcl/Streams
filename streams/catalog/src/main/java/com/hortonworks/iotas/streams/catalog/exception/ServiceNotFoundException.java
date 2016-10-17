package com.hortonworks.iotas.streams.catalog.exception;

import com.hortonworks.iotas.streams.cluster.discovery.ambari.ServiceConfigurations;

public class ServiceNotFoundException extends EntityNotFoundException {
    public ServiceNotFoundException(String message) {
        super(message);
    }

    public ServiceNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public ServiceNotFoundException(Throwable cause) {
        super(cause);
    }

    public ServiceNotFoundException(Long clusterId, ServiceConfigurations service) {
        this("Service [" + service.name() + "] not found in cluster with id [" + clusterId + "]");
    }
}
