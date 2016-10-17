package com.hortonworks.iotas.streams.catalog.service.metadata;

import com.google.common.collect.Lists;

import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import mockit.Injectable;
import mockit.Tested;
import mockit.integration.junit4.JMockit;

@RunWith(JMockit.class)
public class KafkaMetadataServiceTest {

    @Tested
    private KafkaMetadataService kafkaMetadataService;

    @Injectable
    StreamCatalogService catalogService;

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

    @Test
    public void kafkaConnectionWellFormed_Chroot() throws Exception {
//        final String zkStrRaw = "hostname1:port1,hostname2:port2,hostname3:port3/chroot/path";
        final List<String> zkStrs = Lists.newArrayList("hostname1:port1", "hostname1:port1,hostname2:port2,hostname3:port3");
        final List<String> chRoots = Lists.newArrayList("", "/chroot/path/d1/d2", "/chroot/path/d1/d2/");

        for (String zkStr : zkStrs) {
            for (String chRoot : chRoots) {
                final String zkStrRaw = zkStr + chRoot;
                System.out.println("testing: " + zkStrRaw);
                KafkaMetadataService.KafkaZkConnection kafkaZkConnection = KafkaMetadataService.KafkaZkConnection.newInstance(zkStrRaw);
                Assert.assertEquals(zkStr, kafkaZkConnection.getZkString());
                Assert.assertEquals(chRoot.isEmpty() ? "/" : chRoots.get(2), kafkaZkConnection.getChRoot());
            }
        }
    }

    @Test
    public void testCreatePath() throws Exception {
        final String zkStrRaw = "hostname1:port1,hostname2:port2,hostname3:port3/chroot/path/d1/d2";
        KafkaMetadataService.KafkaZkConnection kafkaZkConnection = KafkaMetadataService.KafkaZkConnection.newInstance(zkStrRaw);
        kafkaZkConnection.createPath(KafkaMetadataService.KAFKA_BROKERS_IDS_ZK_RELATIVE_PATH);
        Assert.assertEquals(zkStr, kafkaZkConnection.getZkString());
        Assert.assertEquals(chRoot.isEmpty() ? "/" : chRoots.get(2), kafkaZkConnection.getChRoot());
    }

    void executer(Command command) {
        final List<String> zkStrs = Lists.newArrayList("hostname1:port1", "hostname1:port1,hostname2:port2,hostname3:port3");
        final List<String> chRoots = Lists.newArrayList("", "/chroot/path/d1/d2", "/chroot/path/d1/d2/");

        for (String zkStr : zkStrs) {
            for (String chRoot : chRoots) {
                final String zkStrRaw = zkStr + chRoot;
                System.out.println("testing: " + zkStrRaw);
                command.execute();
                KafkaMetadataService.KafkaZkConnection kafkaZkConnection = KafkaMetadataService.KafkaZkConnection.newInstance(zkStrRaw);
                Assert.assertEquals(zkStr, kafkaZkConnection.getZkString());
                Assert.assertEquals(chRoot.isEmpty() ? "/" : chRoots.get(2), kafkaZkConnection.getChRoot());
            }
        }
    }

    }

    interface Command {
        void execute(String... args);
    }

}