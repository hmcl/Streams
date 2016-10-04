package com.hortonworks.iotas.streams.service.services.metadata;

import com.codahale.metrics.annotation.Timed;
import com.hortonworks.iotas.common.util.WSUtils;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;
import com.hortonworks.iotas.streams.catalog.service.metadata.HBaseMetadataService;

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
public class HBaseMetadataResource {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseMetadataResource.class);
    private final StreamCatalogService catalogService;

    public HBaseMetadataResource(StreamCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    @GET
    @Path("/name/{clusterName}/services/hbase/namespaces")
    @Timed
    public Response getNamespacesByClusterName(@PathParam("clusterName") String clusterName) {
        return getNamespacesByClusterId(catalogService.getClusterByName(clusterName).getId());
    }

    @GET
    @Path("/{clusterId}/services/hbase/namespaces")
    @Timed
    public Response getNamespacesByClusterId(@PathParam("clusterId") Long clusterId) {
        try {
            HBaseMetadataService hbaseMetadataService = HBaseMetadataService.newInstance(catalogService, clusterId);
            final List<String> namespaces = hbaseMetadataService.getHBaseNamespaces();
            if (namespaces != null && !namespaces.isEmpty()) {
                return WSUtils.respond(OK, SUCCESS, namespaces);
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, "No namespaces found for cluster with id [" + clusterId + "]");
    }

    // ===

    @GET
    @Path("/name/{clusterName}/services/hbase/tables")
    @Timed
    public Response getTablesByClusterName(@PathParam("clusterName") String clusterName) {
        return getTablesByClusterId(catalogService.getClusterByName(clusterName).getId());
    }

    @GET
    @Path("/{clusterId}/services/hbase/tables")
    @Timed
    public Response getTablesByClusterId(@PathParam("clusterId") Long clusterId) {
        try {
            HBaseMetadataService hbaseMetadataService = HBaseMetadataService.newInstance(catalogService, clusterId);
            final List<String> tables = hbaseMetadataService.getHBaseTables();
            if (tables != null && !tables.isEmpty()) {
                return WSUtils.respond(OK, SUCCESS, tables);
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, String.format(
                "No tables found for any namespace in cluster with id [%d]", clusterId));
    }

    // ===

    @GET
    @Path("/name/{clusterName}/services/hbase/namespaces/{namespace}/tables")
    @Timed
    public Response getNamespaceTablesByClusterName(@PathParam("clusterName") String clusterName, @PathParam("namespace") String namespace) {
        return getNamespaceTablesByClusterId(catalogService.getClusterByName(clusterName).getId(), namespace);
    }

    @GET
    @Path("/{clusterId}/services/hbase/namespaces/{namespace}/tables")
    @Timed
    public Response getNamespaceTablesByClusterId(@PathParam("clusterId") Long clusterId, @PathParam("namespace") String namespace) {
        try {
            HBaseMetadataService hbaseMetadataService = HBaseMetadataService.newInstance(catalogService, clusterId);
            final List<String> tables = hbaseMetadataService.getHBaseTables(namespace);
            if (tables != null && !tables.isEmpty()) {
                return WSUtils.respond(OK, SUCCESS, tables);
            }
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
        return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, String.format(
                "No tables found for namespace [%s] in cluster with id [%d]", namespace, clusterId));
    }




}
