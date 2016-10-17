package com.hortonworks.iotas.streams.catalog.service.metadata;

import com.google.common.collect.Lists;

import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import mockit.Injectable;
import mockit.Tested;
import mockit.integration.junit4.JMockit;

import static com.hortonworks.iotas.streams.catalog.service.metadata.KafkaMetadataService.KAFKA_BROKERS_IDS_ZK_RELATIVE_PATH;

@RunWith(JMockit.class)
public class KafkaMetadataServiceTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetadataServiceTest.class);

    private static final String CHROOT = "/chroot";
    private static final String PATH = "/path/d1/d2";

    private static final List<String> zkStrs = Lists.newArrayList("hostname1:port1", "hostname1:port1,hostname2:port2,hostname3:port3");
    private static final List<String> chRoots = Lists.newArrayList("", CHROOT + PATH, CHROOT + PATH + "/");

    private static final List<String> expectedChrootPath = Lists.newArrayList("/", CHROOT + PATH + "/");
    private static final List<String> expectedBrokerIdPath = Lists.newArrayList("/" + KAFKA_BROKERS_IDS_ZK_RELATIVE_PATH,
            CHROOT + PATH + "/" + KAFKA_BROKERS_IDS_ZK_RELATIVE_PATH );

    @Tested
    private KafkaMetadataService kafkaMetadataService;

    @Injectable
    StreamCatalogService catalogService;

    @Test
    public void test_KafkaZkConnection_wellInitialized() throws Exception {
        for (String zkStr : zkStrs) {
            for (String chRoot : chRoots) {
                final String zkStrRaw = zkStr + chRoot;
                LOG.debug("zookeeper.connect=" + zkStrRaw);
                KafkaMetadataService.KafkaZkConnection kafkaZkConnection = KafkaMetadataService.KafkaZkConnection.newInstance(zkStrRaw);
                Assert.assertEquals(zkStr, kafkaZkConnection.getZkString());
                Assert.assertEquals(chRoot.isEmpty() ? expectedChrootPath.get(0) : expectedChrootPath.get(1), kafkaZkConnection.getChRoot());
            }
        }
    }

    @Test
    public void test_KafkaZkConnection_createPath() throws Exception {
        for (String zkStr : zkStrs) {
            for (String chRoot : chRoots) {
                final String zkStrRaw = zkStr + chRoot;
                LOG.debug("zookeeper.connect=" + zkStrRaw);
                KafkaMetadataService.KafkaZkConnection kafkaZkConnection = KafkaMetadataService.KafkaZkConnection.newInstance(zkStrRaw);
                final String zkPath = kafkaZkConnection.buildZkFullPath(KAFKA_BROKERS_IDS_ZK_RELATIVE_PATH);
                Assert.assertEquals(chRoot.isEmpty() ? expectedBrokerIdPath.get(0) : expectedBrokerIdPath.get(1), zkPath);
            }
        }
    }

    void createConnection() {

    }

    @Test
    public void getBrokerHostPortFromStreamsJson() throws Exception {

    }

    @Test
    public void getBrokerInfoFromZk() throws Exception {

    }

    @Test
    public void getBrokerIdsFromZk() throws Exception {

    }

    @Test
    public void getTopicsFromZk() throws Exception {

    }




}