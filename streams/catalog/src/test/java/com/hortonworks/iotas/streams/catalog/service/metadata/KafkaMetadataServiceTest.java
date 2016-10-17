package com.hortonworks.iotas.streams.catalog.service.metadata;

import com.google.common.collect.Lists;

import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import mockit.integration.junit4.JMockit;

import static com.hortonworks.iotas.streams.catalog.service.metadata.KafkaMetadataService.KAFKA_BROKERS_IDS_ZK_RELATIVE_PATH;
import static com.hortonworks.iotas.streams.catalog.service.metadata.KafkaMetadataService.KAFKA_TOPICS_ZK_RELATIVE_PATH;

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

    // Mocks
    @Tested
    private KafkaMetadataService kafkaMetadataService;
    @Injectable
    StreamCatalogService catalogService;
    @Injectable
    private ZookeeperClient zkCli;
    @Injectable
    KafkaMetadataService.KafkaZkConnection kafkaZkConnection;

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

    @Test
    public void createConnection() throws Exception {
        final TestingServer server = new TestingServer();
        final String connectionString = server.getConnectString();
        try(ZookeeperClient zookeeperClient = ZookeeperClient.newInstance(connectionString)) {
//            zookeeperClient = ZookeeperClient.newInstance(connectionString);
            zookeeperClient.start();
//        CuratorFramework curatorFrameworkZkCli = zookeeperClient.getCuratorFrameworkZkCli();
////        curatorFrameworkZkCli.create().forPath("");
//        curatorFrameworkZkCli.create().forPath("/hmcl");
            zookeeperClient.createPath("/hmcl");
            zookeeperClient.getChildren("/hmcl");

            System.out.println(connectionString);
        } /*finally {
            if (zookeeperClient != null) {
                zookeeperClient.close();
            }
        }*/
    }

    @Before
    public void setUp() throws Exception {
        final TestingServer server = new TestingServer();
        final String connectionString = server.getConnectString();
        zkCli = ZookeeperClient.newInstance(connectionString);
        zkCli.start();
        kafkaMetadataService = new KafkaMetadataService(catalogService, zkCli, kafkaZkConnection);
    }

    @After
    public void tearDown() {
        zkCli.close();
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
        final String topicsRootZkPath = chRoots.get(1) + "/" + KAFKA_TOPICS_ZK_RELATIVE_PATH;

        final String topic1ZkPath = topicsRootZkPath + "/unit_test_topic_1";
        final String topic2ZkPath = topicsRootZkPath + "/unit_test_topic_2";

        zkCli.createPath(topic1ZkPath);
        zkCli.setData(topic1ZkPath, "topic_1 data".getBytes());
        System.out.println("topic 1 data: " + new String(zkCli.getData(topic1ZkPath)));

        zkCli.createPath(topic2ZkPath);
        zkCli.setData(topic2ZkPath, "topic_2 data".getBytes());
        System.out.println("topic 2 data: " + new String(zkCli.getData(topic2ZkPath)));

        new Expectations() {{
            kafkaZkConnection.buildZkFullPath(anyString); result = topicsRootZkPath;
        }};

        List<String> actualTopics = kafkaMetadataService.getTopicsFromZk().getTopics();
        Collections.sort(actualTopics);
        System.out.println("actual topics: " + actualTopics);
        List<String> expectedTopics = Lists.<String>newArrayList("unit_test_topic_1", "unit_test_topic_2");

        Assert.assertEquals(expectedTopics, actualTopics);
    }




}