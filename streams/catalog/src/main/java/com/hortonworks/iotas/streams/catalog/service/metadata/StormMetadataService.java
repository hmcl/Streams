package com.hortonworks.iotas.streams.catalog.service.metadata;

import com.hortonworks.iotas.streams.catalog.Component;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;
import com.hortonworks.iotas.streams.catalog.service.metadata.common.HostPort;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.AmbariServiceNodeDiscoverer;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.ComponentPropertyPattern;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.ServiceConfigurations;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;

public class StormMetadataService {
    private static final String STREAMS_JSON_SCHEMA_SERVICE_STORM = ServiceConfigurations.STORM.name();
    private static final String STREAMS_JSON_SCHEMA_COMPONENT_STORM_UI_SERVER = ComponentPropertyPattern.STORM_UI_SERVER.name();

    private static final String STORM_REST_API_TOPOLOGIES_DEFAULT_RELATIVE_PATH = "/api/v1/topology/summary";
    private static final String STORM_REST_API_TOPOLOGIES_KEY = "topologies";
    private static final String STORM_REST_API_TOPOLOGY_ID_KEY = "id";

    private Client httpClient;
    private String url;

    public StormMetadataService(Client httpClient, String url) {
        this.httpClient = httpClient;
        this.url = url;
    }

    public static class Builder {
        private StreamCatalogService catalogService;
        private Long clusterId;
        private String urlRelativePath = STORM_REST_API_TOPOLOGIES_DEFAULT_RELATIVE_PATH;
        private String username = "";
        private String password = "";

        public Builder(StreamCatalogService catalogService, Long clusterId) {
            this.catalogService = catalogService;
            this.clusterId = clusterId;
        }

        Builder setUrlRelativePath(String urlRelativePath) {
            this.urlRelativePath = urlRelativePath;
            return this;
        }

        Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public StormMetadataService build() {
            return new StormMetadataService(newHttpClient(), getTopologySummaryRestUrl());
        }

        private Client newHttpClient() {
            final HttpAuthenticationFeature feature = HttpAuthenticationFeature.basicBuilder()
                    .credentials(username, password).build();
            final ClientConfig clientConfig = new ClientConfig();
            clientConfig.register(feature);
            return ClientBuilder.newClient(clientConfig);
        }

        private String getTopologySummaryRestUrl() {
            HostPort hostPort = getHostPort();
            return "http://" + hostPort.toString() + (urlRelativePath.startsWith("/") ? urlRelativePath : "/" + urlRelativePath);
        }

        private HostPort getHostPort() {
            final Component stormUiComp = catalogService.getComponentByName(catalogService.getServiceByClusterId(
                    clusterId, STREAMS_JSON_SCHEMA_SERVICE_STORM).getId(), STREAMS_JSON_SCHEMA_COMPONENT_STORM_UI_SERVER);
            return new HostPort(stormUiComp.getHosts().get(0), stormUiComp.getPort());
        }
    }

    /**
     * @return List of storm topologies as returned by Storm's REST API
     */
    public List<String> getTopologies() {
        final Map<String, ?> jsonAsMap = httpClient.target(url).request(MediaType.APPLICATION_JSON).get(Map.class);
        final List<Map<String, String>> topologiesSummary = (List<Map<String, String>>) jsonAsMap.get(STORM_REST_API_TOPOLOGIES_KEY);
        final List<String> topologies = new ArrayList<>(topologiesSummary.size());
        for (Map<String, String> tpSum : topologiesSummary) {
            topologies.add(tpSum.get(STORM_REST_API_TOPOLOGY_ID_KEY));
        }
        return topologies;
    }
}
