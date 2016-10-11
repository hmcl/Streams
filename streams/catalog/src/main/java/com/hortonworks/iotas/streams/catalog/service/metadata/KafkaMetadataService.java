package com.hortonworks.iotas.streams.catalog.service.metadata;

import com.hortonworks.iotas.streams.catalog.Component;
import com.hortonworks.iotas.streams.catalog.ServiceConfiguration;
import com.hortonworks.iotas.streams.catalog.exception.MissingServiceConfigurationException;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;
import com.hortonworks.iotas.streams.catalog.service.metadata.common.HostPort;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.ComponentPropertyPattern;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.ServiceConfigurations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KafkaMetadataService {
    private static final String STREAMS_JSON_SCHEMA_SERVICE_KAFKA = ServiceConfigurations.KAFKA.name();
    private static final String STREAMS_JSON_SCHEMA_COMPONENT_KAFKA_BROKER = ComponentPropertyPattern.KAFKA_BROKER.name();
    private static final String STREAMS_JSON_SCHEMA_CONFIG_KAFKA_BROKER = ServiceConfigurations.KAFKA.getConfNames()[0];

    private static final String KAFKA_TOPICS_ZK_RELATIVE_PATH = "/brokers/topics";
    private static final String KAFKA_BROKERS_IDS_ZK_RELATIVE_PATH = "/brokers/ids";
    private static final String KAFKA_ZK_CONNECT_PROP = "zookeeper.connect";

    private final StreamCatalogService catalogService;

    public KafkaMetadataService(StreamCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    public List<HostPort> getBrokerHostPortFromStreamsJson(Long clusterId) throws Exception {
        final Component kafkaBrokerComp = catalogService.getComponentByName(getServiceIdByClusterId(clusterId), STREAMS_JSON_SCHEMA_COMPONENT_KAFKA_BROKER);
        List<String> hosts;
        List<HostPort> hostsPorts = null;
        if (kafkaBrokerComp != null) {
            hosts = kafkaBrokerComp.getHostsList();
            final int port = kafkaBrokerComp.getPort();
            hostsPorts = new ArrayList<>(hosts.size());

            for (String host : hosts) {
                hostsPorts.add(new HostPort(host, port));
            }
        }
        return hostsPorts;
    }

    public List<String> getBrokerInfoFromZk(Long clusterId) throws Exception {
        List<String> brokerInfo = null;
        ZookeeperClient zkCli = null;
        try {
            final KafkaZkConnection kafkaZkConnection = createKafkaZkConnection(getZkStringRaw(clusterId));
            zkCli = ZookeeperClient.newInstance(kafkaZkConnection);
            zkCli.start();
            final List<String> brokerIds = zkCli.getChildren(kafkaZkConnection.createPath(KAFKA_BROKERS_IDS_ZK_RELATIVE_PATH));

            if (brokerIds != null) {
                brokerInfo = new ArrayList<>();
                for (String bkId : brokerIds) {
                    final byte[] bytes = zkCli.getData(KAFKA_BROKERS_IDS_ZK_RELATIVE_PATH + "/" + bkId);
                    brokerInfo.add(new String(bytes));
                }
            }
        } finally {
            if (zkCli != null) {
                zkCli.close();
            }
        }
        return brokerInfo;
    }

    public List<String> getBrokerIdsFromZk(Long clusterId) throws Exception {
        final List<String> brokerIds;
        ZookeeperClient zkCli = null;
        try {
            final KafkaZkConnection kafkaZkConnection = createKafkaZkConnection(getZkStringRaw(clusterId));
            zkCli = ZookeeperClient.newInstance(kafkaZkConnection);
            zkCli.start();
            brokerIds = zkCli.getChildren(kafkaZkConnection.createPath(KAFKA_BROKERS_IDS_ZK_RELATIVE_PATH));
        } finally {
            if (zkCli != null) {
                zkCli.close();
            }
        }
        return brokerIds;
    }

    public List<String> getTopicsFromZk(Long clusterId) throws Exception {
        final KafkaZkConnection kafkaZkConnection = createKafkaZkConnection(getZkStringRaw(clusterId));
        final List<String> topics;
        ZookeeperClient zkCli = null;
        try {
            zkCli = ZookeeperClient.newInstance(kafkaZkConnection);
            zkCli.start();
            topics = zkCli.getChildren(kafkaZkConnection.createPath(KAFKA_TOPICS_ZK_RELATIVE_PATH));
        } finally {
            if (zkCli != null) {
                zkCli.close();
            }
        }
        return topics;
    }


    private static class KafkaZkConnection implements ZookeeperClient.ZkConnectionStringFactory {
        String zkString;
        String chRoot;

        private KafkaZkConnection(String zkString, String chRoot) {
            this.zkString = zkString;
            this.chRoot = chRoot;
        }

        @Override
        public String createZkConnString() {
            return zkString;
        }

        public String createPath(String zkRelativePath) {
            if (zkRelativePath.startsWith("/")) {
                return chRoot + zkRelativePath.substring(1);
            } else {
                return chRoot + zkRelativePath;
            }

        }
    }

    // zk string as defined in the broker zk property
    private KafkaZkConnection createKafkaZkConnection(String zkStringRaw) {
        final String[] split = zkStringRaw.split("/", 2);
        String zkString = "";
        String chRoot = "";

        zkString = split[0];
        if (split.length > 1) {
            chRoot = "/" + split[1];
            if (!chRoot.endsWith("/")) {
                chRoot = chRoot + "/";
            }
        } else {
            chRoot = "/";
        }
        return new KafkaZkConnection(zkString, chRoot);
    }

    private String getZkStringRaw(Long clusterId) throws IOException, MissingServiceConfigurationException {
        final ServiceConfiguration kafkaBrokerConfig = catalogService.getServiceConfigurationByName(
                getServiceIdByClusterId(clusterId), STREAMS_JSON_SCHEMA_CONFIG_KAFKA_BROKER);
        if (kafkaBrokerConfig != null) {
            return kafkaBrokerConfig.getConfigurationMap().get(KAFKA_ZK_CONNECT_PROP);
        } else {
            throw new MissingServiceConfigurationException("Required " + STREAMS_JSON_SCHEMA_CONFIG_KAFKA_BROKER + " configuration not found");
        }
    }

    private Long getServiceIdByClusterId(Long clusterId) {
        return catalogService.getServiceByClusterId(clusterId, STREAMS_JSON_SCHEMA_SERVICE_KAFKA).getId();
    }

}
