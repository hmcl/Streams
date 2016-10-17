package com.hortonworks.iotas.streams.catalog.service.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.hortonworks.iotas.streams.catalog.exception.ServiceConfigurationNotFoundException;
import com.hortonworks.iotas.streams.catalog.exception.ServiceNotFoundException;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;
import com.hortonworks.iotas.streams.catalog.service.metadata.common.OverrideHadoopConfiguration;
import com.hortonworks.iotas.streams.catalog.service.metadata.common.Tables;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.ServiceConfigurations;

import org.apache.calcite.sql.parser.SqlParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hdfs.DFSClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Tested;
import mockit.integration.junit4.JMockit;

@RunWith(JMockit.class)
public class HBaseMetadataServiceTest {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseMetadataServiceTest.class);

    @Tested
    private HBaseMetadataService hBaseMetadataService;
    @Injectable
    private Admin hBaseAdmin;

    /*@Before
    public void setUp() throws Exception {
        hBaseMetadataService = new HBaseMetadataService(
                ConnectionFactory.createConnection(HBaseConfiguration.create()).getAdmin());
    }*/

    /*@Before
    public void setUp() throws Exception {
        hBaseMetadataService = new HBaseMetadataService(
                ConnectionFactory.createConnection(HBaseConfiguration.create()).getAdmin());
    }*/

    final class MyMock<T extends Configuration> extends MockUp<OverrideHadoopConfiguration> {
        T config;

        public MyMock() {
        }

        public MyMock(T config) {
            log("Created MyMock");
            this.config = config;
        }

        /*public static <K extends Configuration> MyMock<K> newInstance() throws Exception {
            final HBaseConfigurationTest config = new HBaseConfigurationTest(HBaseConfiguration.create());
            overrideProps(config);
            return new MyMock<K>((K) config);
        }*/

        private void overrideProps(HBaseConfigurationTest config) throws Exception {
//            log("newInstance Before: " + config.getProps());
            final List<Map<String, String>> newProps = getHbaseSiteXmlProps();
            for (Map<String, String> newProp : newProps) {
                assert config.getProps() != null;
                if (newProp != null) {
                    String name = newProp.get("name");
                    String value = newProp.get("value");
                    if (name != null && value != null) {
                        config.getProps().put(name, value);
                        log(String.format("Set Property (%s,%s)", name, value));
                    } else {
                        log(String.format("NULL property (%s,%s)", name, value));
                    }
                }
            }
//            log("newInstance After: " + config.getProps());
        }

        private List<Map<String, String>> getHbaseSiteXmlProps() throws Exception {
            final ObjectMapper mapper = new XmlMapper();
            List<Map<String, String>> props = mapper.readValue(getHbaseSiteXmlFileInputStream(), new TypeReference<List<Map<String, String>>>() { });
            LOG.debug("hbase-site props: {}", props);
            return  props;
        }

        private FileInputStream getHbaseSiteXmlFileInputStream() throws FileNotFoundException {
            final String filePath = "/Users/hlouro/Hortonworks/Tasks/IoTaS/ClusterMetadata/ConfigFiles/hbase-site.xml";
            return new FileInputStream(filePath);
        }


        @Mock T override(T configuration, StreamCatalogService catalogService,
                         ServiceConfigurations service, Long clusterId, String configurationName) {
            
            final HBaseConfigurationTest testConfig = new HBaseConfigurationTest(configuration);
            
            log("override Before: " + testConfig.getProps());

            try {
                overrideProps(testConfig);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            log("override After: " + testConfig.getProps());
            return (T) testConfig;
//            return configuration;
        }

        void log(String msg, String... args) {
            System.out.println(Thread.currentThread().getName() + " - " + Integer.toHexString(System.identityHashCode(this)) + " - " + String.format(msg, args));
        }
    }




    final class ConnFactoryMock extends MockUp<ConnectionFactory> {
        @Mock Connection createConnection(Configuration conf) throws IOException {
            return null;
        }
    }

    private static class HBaseConfigurationTest extends HBaseConfiguration {

        public HBaseConfigurationTest(Configuration c) {
            super(c);
        }

        @Override
        public synchronized Properties getProps() {
            return super.getProps();
        }
    }


    @Test
    public void testMock(@Mocked StreamCatalogService catalogService) throws Exception {
//        new ConnFactoryMock();
//        MyMock.<HBaseConfigurationTest>newInstance();
        new MyMock<HBaseConfigurationTest>();
//        OverrideHadoopConfiguration.override(null, null, null, null, null);   // does not work
        HBaseMetadataService.newInstance(catalogService, 1L);
    }

    @Test
    public void testMock1(@Mocked final StreamCatalogService catalogService,
                          @Mocked OverrideHadoopConfiguration overrideHadoopConfiguration) throws Exception {

        new Expectations() {{
            OverrideHadoopConfiguration.override((Configuration) any, catalogService, ServiceConfigurations.HBASE, 123L, "hbase-site");
            result = getConfigurationTest();
        }};

        HBaseMetadataService.newInstance(catalogService, 1L);
    }

    HBaseConfigurationTest getConfigurationTest() throws Exception {
        HBaseConfigurationTest config = new HBaseConfigurationTest(HBaseConfiguration.create());
        overrideProps1(config);
        return config;
    }

    private void overrideProps1(HBaseConfigurationTest config) throws Exception {
//            log("newInstance Before: " + config.getProps());
        final List<Map<String, String>> newProps = getHbaseSiteXmlProps();
        for (Map<String, String> newProp : newProps) {
            assert config.getProps() != null;
            if (newProp != null) {
                String name = newProp.get("name");
                String value = newProp.get("value");
                if (name != null && value != null) {
                    config.getProps().put(name, value);
                    log1(String.format("Set Property (%s,%s)", name, value));
                } else {
                    log1(String.format("NULL property (%s,%s)", name, value));
                }
            }
        }
//            log("newInstance After: " + config.getProps());
    }

    private List<Map<String, String>> getHbaseSiteXmlProps1() throws Exception {
        final ObjectMapper mapper = new XmlMapper();
        List<Map<String, String>> props = mapper.readValue(getHbaseSiteXmlFileInputStream(), new TypeReference<List<Map<String, String>>>() { });
        LOG.debug("hbase-site props: {}", props);
        return  props;
    }

    private FileInputStream getHbaseSiteXmlFileInputStream1() throws FileNotFoundException {
        final String filePath = "/Users/hlouro/Hortonworks/Tasks/IoTaS/ClusterMetadata/ConfigFiles/hbase-site.xml";
        return new FileInputStream(filePath);
    }

    void log1(String msg, String... args) {
        System.out.println(Thread.currentThread().getName() + " - " + Integer.toHexString(System.identityHashCode(this)) + " - " + String.format(msg, args));
    }

    // ===

    @Test
    public void getHBaseTables() throws Exception {
        Tables hBaseTables = hBaseMetadataService.getHBaseTables();
        System.out.println("Stop");
    }

    @Test
    public void getHBaseTablesForNamespace() throws Exception {
        Tables hBaseTables = hBaseMetadataService.getHBaseTables("ns1");
        System.out.println("Stop");
    }

    @Test
    public void getHBaseNamespaces() throws Exception {
        HBaseMetadataService.Namespaces hBaseNamespaces = hBaseMetadataService.getHBaseNamespaces();
        System.out.println("Stop");
    }

    @Test
    public void testgetHbaseSiteXmlProps() throws Exception {
        getHbaseSiteXmlProps();
    }

    public List<Map<String, String>> getHbaseSiteXmlProps() throws Exception {
        final ObjectMapper mapper = new XmlMapper();
        List<Map<String, String>> props = mapper.readValue(getHbaseSiteXmlFileInputStream(), new TypeReference<List<Map<String, String>>>() { });
        LOG.debug("hbase-site props: {}", props);
        System.out.printf("hbase-site props: %s", props);
        return  props;
    }

    @Test
    public void readHBaseSite() throws Exception {
        ObjectMapper mapper = new XmlMapper();
        JsonNode jsonTree = mapper.readTree(getHbaseSiteXmlFileInputStream());
        System.out.println(jsonTree.asText());
    }

    private FileInputStream getHbaseSiteXmlFileInputStream() throws FileNotFoundException {
        final String filePath = "/Users/hlouro/Hortonworks/Tasks/IoTaS/ClusterMetadata/ConfigFiles/hbase-site.xml";
        return new FileInputStream(filePath);
    }
}