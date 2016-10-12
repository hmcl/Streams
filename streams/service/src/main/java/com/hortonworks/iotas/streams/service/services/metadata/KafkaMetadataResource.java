package com.hortonworks.iotas.streams.service.services.metadata;

import com.codahale.metrics.annotation.Timed;
import com.hortonworks.iotas.common.util.WSUtils;
    import com.hortonworks.iotas.streams.catalog.Cluster;
import com.hortonworks.iotas.streams.catalog.exception.EntityNotFoundException;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;
import com.hortonworks.iotas.streams.catalog.service.metadata.common.HostPort;
import com.hortonworks.iotas.streams.catalog.service.metadata.KafkaMetadataService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.hortonworks.iotas.common.catalog.CatalogResponse.ResponseMessage.ENTITY_BY_NAME_NOT_FOUND;
import static com.hortonworks.iotas.common.catalog.CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND;
import static com.hortonworks.iotas.common.catalog.CatalogResponse.ResponseMessage.EXCEPTION;
import static com.hortonworks.iotas.common.catalog.CatalogResponse.ResponseMessage.SUCCESS;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

@Path("/api/v1/catalog/clusters")
@Produces(MediaType.APPLICATION_JSON)
public class KafkaMetadataResource {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetadataResource.class);
    private final StreamCatalogService catalogService;

    public KafkaMetadataResource(StreamCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    @GET
    @Path("/name/{clusterName}/services/kafka/brokers")
    @Timed
    public Response getBrokersByClusterName(@PathParam("clusterName") String clusterName) {
        final Cluster cluster = catalogService.getClusterByName(clusterName);
        if (cluster == null) {
            return WSUtils.respond(NOT_FOUND, ENTITY_BY_NAME_NOT_FOUND, "cluster name " + clusterName);
        }
        return getBrokersByClusterId(cluster.getId());
    }

    @GET
    @Path("/{clusterId}/services/kafka/brokers")
    @Timed
    public Response getBrokersByClusterId(@PathParam("clusterId") Long clusterId) {
        try {
            final KafkaMetadataService kafkaMetadataService = new KafkaMetadataService(catalogService);
            final List<HostPort> hostsPorts = kafkaMetadataService.getBrokerHostPortFromStreamsJson(clusterId);
            if (hostsPorts != null && !hostsPorts.isEmpty()) {
                return WSUtils.respond(OK, SUCCESS, hostsPorts);
            } else {
                throw new EntityNotFoundException("No Kafka brokers found for cluster [" + clusterId + "]");
            }
        } catch (EntityNotFoundException ex) {
            return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, ex.getMessage());
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }

    @GET
    @Path("/name/{clusterName}/services/kafka/topics")
    @Timed
    public Response getTopicsByClusterName(@PathParam("clusterName") String clusterName) {
        final Cluster cluster = catalogService.getClusterByName(clusterName);
        if (cluster == null) {
            return WSUtils.respond(NOT_FOUND, ENTITY_BY_NAME_NOT_FOUND, "cluster name " + clusterName);
        }

        return getTopicsByClusterId(cluster.getId());
    }

    @GET
    @Path("/{clusterId}/services/kafka/topics")
    @Timed
    public Response getTopicsByClusterId(@PathParam("clusterId") Long clusterId) {
        try {
            final KafkaMetadataService kafkaMetadataService = new KafkaMetadataService(catalogService);
            final List<String> brokerInfo = kafkaMetadataService.getTopicsFromZk(clusterId);
            if (brokerInfo != null) {
                return WSUtils.respond(OK, SUCCESS, brokerInfo);
            } else {
                throw new EntityNotFoundException("No Kafka brokers found for cluster [" + clusterId + "]");
            }
        } catch (EntityNotFoundException ex) {
            return WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, ex.getMessage());
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }
}
