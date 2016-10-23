package org.apache.streamline.streams.service.metadata;

import org.apache.streamline.common.util.FileStorage;
import org.apache.streamline.storage.StorageManager;
import org.apache.streamline.streams.catalog.service.StreamCatalogService;
import org.apache.streamline.streams.layout.component.TopologyActions;
import org.apache.streamline.streams.metrics.topology.TopologyMetrics;

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