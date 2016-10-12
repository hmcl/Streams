package com.hortonworks.iotas.streams.catalog.service.metadata;

import com.hortonworks.iotas.streams.catalog.Component;
import com.hortonworks.iotas.streams.catalog.ServiceConfiguration;
import com.hortonworks.iotas.streams.catalog.exception.ServiceComponentNotFoundException;
import com.hortonworks.iotas.streams.catalog.exception.ServiceConfigurationNotFoundException;
import com.hortonworks.iotas.streams.catalog.exception.ServiceNotFoundException;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;
import com.hortonworks.iotas.streams.catalog.service.metadata.common.HostPort;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.ComponentPropertyPattern;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.ServiceConfigurations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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

    public BrokersHostPort getBrokerHostPortFromStreamsJson(Long clusterId) throws ServiceNotFoundException, ServiceComponentNotFoundException {
        final Component kafkaBrokerComp = getComponentByName(clusterId);
        return BrokersHostPort.newInstance(kafkaBrokerComp.getHostsList(), kafkaBrokerComp.getPort());
    }

    public List<String> getBrokerInfoFromZk(Long clusterId) throws Exception {
        List<String> brokerInfo = null;
        ZookeeperClient zkCli = null;
        try {
            final KafkaZkConnection kafkaZkConnection = KafkaZkConnection.newInstance(getZkStringRaw(clusterId));
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
            final KafkaZkConnection kafkaZkConnection = KafkaZkConnection.newInstance(getZkStringRaw(clusterId));
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

    public static class BrokerIds {

    }

    public List<String> getTopicsFromZk(Long clusterId) throws Exception {
        final KafkaZkConnection kafkaZkConnection = KafkaZkConnection.newInstance(getZkStringRaw(clusterId));
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

    private String getZkStringRaw(Long clusterId) throws IOException, ServiceConfigurationNotFoundException, ServiceNotFoundException {
        final ServiceConfiguration kafkaBrokerConfig = catalogService.getServiceConfigurationByName(
                getServiceIdByClusterId(clusterId), STREAMS_JSON_SCHEMA_CONFIG_KAFKA_BROKER);
        if (kafkaBrokerConfig == null || kafkaBrokerConfig.getConfigurationMap() == null)  {
            throw new ServiceConfigurationNotFoundException(clusterId, ServiceConfigurations.KAFKA, STREAMS_JSON_SCHEMA_CONFIG_KAFKA_BROKER);
        }
        return kafkaBrokerConfig.getConfigurationMap().get(KAFKA_ZK_CONNECT_PROP);
    }

    private Component getComponentByName(Long clusterId) throws ServiceNotFoundException, ServiceComponentNotFoundException {
        Component component = catalogService.getComponentByName(getServiceIdByClusterId(clusterId), STREAMS_JSON_SCHEMA_COMPONENT_KAFKA_BROKER);
        if (component == null) {
            throw new ServiceComponentNotFoundException(clusterId, ServiceConfigurations.KAFKA, ComponentPropertyPattern.KAFKA_BROKER);
        }
        return component;
    }

    private Long getServiceIdByClusterId(Long clusterId) throws ServiceNotFoundException {
        Long serviceId = catalogService.getServiceIdByClusterId(clusterId, STREAMS_JSON_SCHEMA_SERVICE_KAFKA);
        if (serviceId == null) {
            throw new ServiceNotFoundException(clusterId, ServiceConfigurations.KAFKA);
        }
        return serviceId;
    }

    /** Wrapper used to show proper JSON formatting
     * {@code
     * {
     *  "brokers" : [ {
     *    "host" : "H1",
     *    "port" : 23
     *   }, {
     *    "host" : "H2",
     *    "port" : 23
     *   },{
     *    "host" : "H3",
     *    "port" : 23
     *   } ]
     *  }
     *}
     * */
    public static class BrokersHostPort {
        private List<HostPort> brokers;

        public BrokersHostPort(List<HostPort> brokers) {
            this.brokers = brokers;
        }

        public static BrokersHostPort newInstance(List<String> hosts, Integer port) {
            List<HostPort> hostsPorts = Collections.emptyList();
            if (hosts != null) {
                hostsPorts = new ArrayList<>(hosts.size());
                for (String host : hosts) {
                    hostsPorts.add(new HostPort(host, port));
                }
            }
            return new BrokersHostPort(hostsPorts);
        }

        public List<HostPort> getBrokers() {
            return brokers;
        }
    }

    public static class BrokersHostPort<T> {
        private List<T> brokers;

        public BrokersHostPort(List<T> brokers) {
            this.brokers = brokers;
        }

        @SuppressWarnings("unchecked")
        public static <T> T newInstance(List<String> hosts, Integer port) {
            List<T> hostsPorts = Collections.emptyList();
            if (hosts != null) {
                hostsPorts = new ArrayList<>(hosts.size());
                for (String host : hosts) {
                    hostsPorts.add((T) new HostPort(host, port));
                }
            }
            return (T) new BrokersHostPort(hostsPorts);
        }

        public List<T> getBrokers() {
            return brokers;
        }
    }

    private static class KafkaZkConnection implements ZookeeperClient.ZkConnectionStringFactory {
        String zkString;
        String chRoot;

        private KafkaZkConnection(String zkString, String chRoot) {
            this.zkString = zkString;
            this.chRoot = chRoot;
        }

        /**
         * Factory method
         * @param zkStringRaw zk connection string as defined in the broker zk property
         * */
        static KafkaZkConnection newInstance(String zkStringRaw) {
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

}
