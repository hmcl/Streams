package com.hortonworks.iotas.streams.service.services.metadata;

import com.codahale.metrics.annotation.Timed;
import com.hortonworks.iotas.common.util.WSUtils;
import com.hortonworks.iotas.streams.catalog.Service;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;
import com.hortonworks.iotas.streams.catalog.service.metadata.StormMetadataService;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.AmbariServiceNodeDiscoverer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.hortonworks.iotas.common.catalog.CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND;
import static com.hortonworks.iotas.common.catalog.CatalogResponse.ResponseMessage.EXCEPTION;
import static com.hortonworks.iotas.common.catalog.CatalogResponse.ResponseMessage.SUCCESS;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

@Path("/api/v1/catalog/clusters")
@Produces(MediaType.APPLICATION_JSON)
public class StormMetadataResource {
    private static final Logger LOG = LoggerFactory.getLogger(StormMetadataResource.class);
    private final StreamCatalogService catalogService;

    public StormMetadataResource(StreamCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    @GET
    @Path("/name/{clusterName}/services/storm/topologies")
    @Timed
    public Response getTopologiesByClusterName(@PathParam("clusterName") String clusterName) {
        return getTopologiesByClusterId(catalogService.getClusterByName(clusterName).getId());
    }

    @GET
    @Path("/{clusterId}/services/storm/topologies")
    @Timed
    public Response getTopologiesByClusterId(@PathParam("clusterId") Long clusterId) {
        try {
            StormMetadataService stormMetadataService = new StormMetadataService.Builder(catalogService, clusterId).build();
            final List<String> topologies = stormMetadataService.getTopologies();
            if (topologies != null && !topologies.isEmpty()) {
                return WSUtils.respond(OK, SUCCESS, topologies);
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, "No topologies found for cluster with id [" + clusterId + "]");
    }
}
