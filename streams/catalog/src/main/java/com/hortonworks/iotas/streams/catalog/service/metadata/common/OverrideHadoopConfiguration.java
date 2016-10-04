package com.hortonworks.iotas.streams.catalog.service.metadata.common;

import com.hortonworks.iotas.streams.catalog.ServiceConfiguration;
import com.hortonworks.iotas.streams.catalog.exception.ServiceConfigurationNotFoundException;
import com.hortonworks.iotas.streams.catalog.exception.ServiceNotFoundException;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.ServiceConfigurations;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class OverrideHadoopConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(OverrideHadoopConfiguration.class);

    public static <T extends Configuration> T override(T configuration, StreamCatalogService catalogService,
            ServiceConfigurations service, Long clusterId, String configurationName)
                throws IOException, ServiceConfigurationNotFoundException, ServiceNotFoundException {

        final ServiceConfiguration serviceConfig = catalogService.getServiceConfigurationByName(
                getServiceIdByClusterId(catalogService, clusterId, service), configurationName);

        if (serviceConfig != null) {
            final Map<String, String> configurationMap = serviceConfig.getConfigurationMap();
            if (configurationMap != null) {
                for (Map.Entry<String, String> propKeyVal : configurationMap.entrySet()) {
                    configuration.set(propKeyVal.getKey(), propKeyVal.getValue());
                    LOG.debug("Set property {}", propKeyVal);
                }
                return configuration;
            }
        }
        throw new ServiceConfigurationNotFoundException("Required [" + configurationName +
                "] configuration not found for service [" + service.name() + "]");
    }

    private static Long getServiceIdByClusterId(StreamCatalogService catalogService, Long clusterId,
            ServiceConfigurations service) throws ServiceNotFoundException {

        final Long serviceId = catalogService.getServiceIdByClusterId(clusterId, service.name());
        if (serviceId == null) {
            throw new ServiceNotFoundException(clusterId, service);
        }
        return serviceId;
    }
}
