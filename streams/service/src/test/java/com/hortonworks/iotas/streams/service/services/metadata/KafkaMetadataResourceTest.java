package com.hortonworks.iotas.streams.service.services.metadata;

import com.hortonworks.iotas.common.util.FileStorage;
import com.hortonworks.iotas.storage.StorageManager;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;
import com.hortonworks.iotas.streams.layout.component.TopologyActions;
import com.hortonworks.iotas.streams.metrics.topology.TopologyMetrics;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import mockit.Injectable;
import mockit.Tested;
import mockit.integration.junit4.JMockit;

@RunWith(JMockit.class)
public class KafkaMetadataResourceTest {
    @Tested
    private KafkaMetadataResource kafkaMetadataResource;
    @Injectable
    private StreamCatalogService catalogService;
    @Injectable
    private StorageManager dao;
    @Injectable
    TopologyActions topologyActions;
    @Injectable
    TopologyMetrics topologyMetrics;
    @Injectable
    FileStorage fileStorage;

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void getConfig() throws Exception {

    }

    @Test
    public void getBrokers() throws Exception {
        kafkaMetadataResource.getBrokersByClusterId(1L);
    }

    @Test
    public void getTopics() throws Exception {
        kafkaMetadataResource.getTopicsByClusterId(1L);
    }

}