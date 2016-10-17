package com.hortonworks.iotas.streams.catalog.service.metadata;

import com.hortonworks.iotas.streams.catalog.Component;
import com.hortonworks.iotas.streams.catalog.ServiceConfiguration;
import com.hortonworks.iotas.streams.catalog.exception.ServiceComponentNotFoundException;
import com.hortonworks.iotas.streams.catalog.exception.ServiceConfigurationNotFoundException;
import com.hortonworks.iotas.streams.catalog.exception.ServiceNotFoundException;
import com.hortonworks.iotas.streams.catalog.exception.ZookeeperClientException;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;
import com.hortonworks.iotas.streams.catalog.service.metadata.common.HostPort;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.ComponentPropertyPattern;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.ServiceConfigurations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KafkaMetadataService {
    public static final String STREAMS_JSON_SCHEMA_SERVICE_KAFKA = ServiceConfigurations.KAFKA.name();
    public static final String STREAMS_JSON_SCHEMA_COMPONENT_KAFKA_BROKER = ComponentPropertyPattern.KAFKA_BROKER.name();
    public static final String STREAMS_JSON_SCHEMA_CONFIG_KAFKA_BROKER = ServiceConfigurations.KAFKA.getConfNames()[0];

    public static final String KAFKA_TOPICS_ZK_RELATIVE_PATH = "brokers/topics";
    public static final String KAFKA_BROKERS_IDS_ZK_RELATIVE_PATH = "brokers/ids";
    public static final String KAFKA_ZK_CONNECT_PROP = "zookeeper.connect";

    private final StreamCatalogService catalogService;

    public KafkaMetadataService(StreamCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    public BrokersInfo<HostPort> getBrokerHostPortFromStreamsJson(Long clusterId) throws ServiceNotFoundException, ServiceComponentNotFoundException {
        final Component kafkaBrokerComp = getComponentByName(clusterId);
        return BrokersInfo.hostPort(kafkaBrokerComp.getHosts(), kafkaBrokerComp.getPort());
    }

    public BrokersInfo<String> getBrokerInfoFromZk(Long clusterId)
            throws ServiceConfigurationNotFoundException, IOException, ServiceNotFoundException, ZookeeperClientException {

        List<String> brokerInfo = null;
        ZookeeperClient zkCli = null;
        try {
            final KafkaZkConnection kafkaZkConnection = KafkaZkConnection.newInstance(getZkStringRaw(clusterId));
            zkCli = ZookeeperClient.newInstance(kafkaZkConnection);
            zkCli.start();
            final String brokerIdsZkPath = kafkaZkConnection.buildZkFullPath(KAFKA_BROKERS_IDS_ZK_RELATIVE_PATH);
            final List<String> brokerIds = zkCli.getChildren(brokerIdsZkPath);

            if (brokerIds != null) {
                brokerInfo = new ArrayList<>();
                for (String bkId : brokerIds) {
                    final byte[] bytes = zkCli.getData(brokerIdsZkPath + "/" + bkId);
                    brokerInfo.add(new String(bytes));
                }
            }
        } finally {
            if (zkCli != null) {
                zkCli.close();
            }
        }
        return BrokersInfo.fromZk(brokerInfo);
    }

    public BrokersInfo<BrokersInfo.BrokerId> getBrokerIdsFromZk(Long clusterId)
            throws ServiceConfigurationNotFoundException, IOException, ServiceNotFoundException, ZookeeperClientException {

        final List<String> brokerIds;
        ZookeeperClient zkCli = null;
        try {
            final KafkaZkConnection kafkaZkConnection = KafkaZkConnection.newInstance(getZkStringRaw(clusterId));
            zkCli = ZookeeperClient.newInstance(kafkaZkConnection);
            zkCli.start();
            brokerIds = zkCli.getChildren(kafkaZkConnection.buildZkFullPath(KAFKA_BROKERS_IDS_ZK_RELATIVE_PATH));
        } finally {
            if (zkCli != null) {
                zkCli.close();
            }
        }
        return BrokersInfo.brokerIds(brokerIds);
    }

    public Topics getTopicsFromZk(Long clusterId)
            throws ServiceConfigurationNotFoundException, IOException, ServiceNotFoundException, ZookeeperClientException {

        final KafkaZkConnection kafkaZkConnection = KafkaZkConnection.newInstance(getZkStringRaw(clusterId));
        final List<String> topics;
        ZookeeperClient zkCli = null;
        try {
            zkCli = ZookeeperClient.newInstance(kafkaZkConnection);
            zkCli.start();
            topics = zkCli.getChildren(kafkaZkConnection.buildZkFullPath(KAFKA_TOPICS_ZK_RELATIVE_PATH));
        } finally {
            if (zkCli != null) {
                zkCli.close();
            }
        }
        return topics == null ? new Topics(Collections.<String>emptyList()) : new Topics(topics);
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
     *
     *  {
     *   "brokers" : [ {
     *     "id" : "1"
     *   }, {
     *     "id" : "2"
     *   }, {
     *     "id" : "3"
     *   } ]
     *   }
     *}
     * */

    public static class BrokersInfo<T> {
        private List<T> brokers;

        public BrokersInfo(List<T> brokers) {
            this.brokers = brokers;
        }

        public static BrokersInfo<HostPort> hostPort(List<String> hosts, Integer port) {
            List<HostPort> hostsPorts = Collections.emptyList();
            if (hosts != null) {
                hostsPorts = new ArrayList<>(hosts.size());
                for (String host : hosts) {
                    hostsPorts.add(new HostPort(host, port));
                }
            }
            return new BrokersInfo<>(hostsPorts);
        }

        public static BrokersInfo<BrokerId> brokerIds(List<String> brokerIds) {
            List<BrokerId> brokerIdsType = Collections.emptyList();
            if (brokerIds != null) {
                brokerIdsType = new ArrayList<>(brokerIds.size());
                for (String brokerId : brokerIds) {
                    brokerIdsType.add(new BrokerId(brokerId));
                }
            }
            return new BrokersInfo<>(brokerIdsType);
        }

        public static BrokersInfo<String> fromZk(List<String> brokerInfo) {
            return brokerInfo == null
                    ? new BrokersInfo<>(Collections.<String>emptyList())
                    : new BrokersInfo<>(brokerInfo);
        }

        public List<T> getBrokers() {
            return brokers;
        }

        public static class BrokerId {
            String id;

            public BrokerId(String id) {
                this.id = id;
            }
        }
    }

    /**
     * Wrapper used to show proper JSON formatting
     */
    public static class Topics {
        List<String> topics;

        public Topics(List<String> topics) {
            this.topics = topics;
        }

        public List<String> getTopics() {
            return topics;
        }
    }

    static class KafkaZkConnection implements ZookeeperClient.ZkConnectionStringFactory {
        String zkString;
        String chRoot;

        KafkaZkConnection(String zkString, String chRoot) {
            this.zkString = zkString;
            this.chRoot = chRoot;
        }

        /**
         * Factory method to create instance of {@link KafkaZkConnection} taking into consideration chRoot
         * @param zkStringRaw zk connection string as defined in the broker zk property. It has the pattern
         *                    "hostname1:port1,hostname2:port2,hostname3:port3/chroot/path"
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

        public String buildZkFullPath(String zkRelativePath) {
            if (zkRelativePath.startsWith("/")) {
                return chRoot + zkRelativePath.substring(1);
            } else {
                return chRoot + zkRelativePath;
            }
        }

        String getZkString() {
            return zkString;
        }

        String getChRoot() {
            return chRoot;
        }
    }

}
