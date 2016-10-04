package com.hortonworks.iotas.streams.catalog.service.metadata.common;

import com.hortonworks.iotas.streams.catalog.ServiceConfiguration;
import com.hortonworks.iotas.streams.catalog.exception.MissingServiceConfigurationException;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class OverrideHadoopConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(OverrideHadoopConfiguration.class);

    public static <T extends Configuration> T override(T configuration, StreamCatalogService catalogService, String serviceName, Long clusterId,
                                                       String configurationName) throws IOException, MissingServiceConfigurationException {

        final ServiceConfiguration serviceConfig = catalogService.getServiceConfigurationByName(
                catalogService.getServiceIdByClusterId(clusterId, serviceName), configurationName);

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
        throw new MissingServiceConfigurationException("Required [" + configurationName +
                "] configuration not found for service [" + serviceName + "]");
    }
}
