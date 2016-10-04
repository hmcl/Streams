package com.hortonworks.iotas.streams.service.services.metadata;

import com.codahale.metrics.annotation.Timed;
import com.hortonworks.iotas.common.util.WSUtils;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;
import com.hortonworks.iotas.streams.catalog.service.metadata.HiveMetadataService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
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
public class HiveMetadataResource {
    private static final Logger LOG = LoggerFactory.getLogger(HiveMetadataResource.class);
    private final StreamCatalogService catalogService;

    public HiveMetadataResource(StreamCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    @GET
    @Path("/name/{clusterName}/services/hive/databases")
    @Timed
    public Response getDatabasesByClusterName(@PathParam("clusterName") String clusterName) {
        return getDatabasesByClusterId(catalogService.getClusterByName(clusterName).getId());
    }

    @GET
    @Path("/{clusterId}/services/hive/databases")
    @Timed
    public Response getDatabasesByClusterId(@PathParam("clusterId") Long clusterId) {
        try {
            HiveMetadataService hiveMetadataService = HiveMetadataService.newInstance(catalogService, clusterId);
            final List<String> databases = hiveMetadataService.getHiveDatabases();
            if (databases != null && !databases.isEmpty()) {
                return WSUtils.respond(OK, SUCCESS, databases);
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, "No databases found for cluster with id [" + clusterId + "]");
    }

    @GET
    @Path("/name/{clusterName}/services/hive/databases/{dbName}/tables")
    @Timed
    public Response getDatabaseTablesByClusterName(@PathParam("clusterName") String clusterName, @PathParam("dbName") String dbName) {
        return getDatabaseTablesByClusterId(catalogService.getClusterByName(clusterName).getId(), dbName);
    }

    @GET
    @Path("/{clusterId}/services/hive/databases/{dbName}/tables")
    @Timed
    public Response getDatabaseTablesByClusterId(@PathParam("clusterId") Long clusterId, @PathParam("dbName") String dbName) {
        try {
            HiveMetadataService hiveMetadataService = HiveMetadataService.newInstance(catalogService, clusterId);
            final List<String> tables = hiveMetadataService.getHiveTables(dbName);
            if (tables != null && !tables.isEmpty()) {
                return WSUtils.respond(OK, SUCCESS, tables);
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, String.format(
                "No tables found for database [%s] in cluster with id [%d]", dbName, clusterId));
    }
}
