package com.hortonworks.iotas.streams.catalog.service.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.iotas.streams.catalog.ServiceConfiguration;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;
import com.hortonworks.iotas.streams.catalog.service.metadata.common.Tables;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import mockit.Expectations;
import mockit.MockUp;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;

@RunWith(JMockit.class)
public class HBaseMetadataServiceTest {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseMetadataServiceTest.class);

    private HBaseMetadataService hbaseService;

    @Mocked
    private StreamCatalogService catalogService;
    @Mocked
    private ServiceConfiguration serviceConfiguration;

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

    class ServiceConfigurationMockUp extends MockUp<ServiceConfiguration> {

        Map<String, String> getConfigurationMap() throws IOException {
            return getHBaseConfiProps();
        }
    }


    @Test
    public void testMock2(/*@Mocked final StreamCatalogService catalogService, @Mocked ServiceConfiguration serviceConfiguration*/) throws Exception {

        new Expectations() {{
            serviceConfiguration.getConfigurationMap(); result = getHBaseConfiProps();
        }};

//        new ServiceConfigurationMockUp();

        /*HBaseMetadataService*/ hbaseService = HBaseMetadataService.newInstance(catalogService, 1L);

        final String namespace = "test_namespace";
        hbaseService.createNamespace(namespace);

        hbaseService.createTable(namespace, "test_table_1", "family");
        hbaseService.createTable(namespace, "test_table_2", "family");
        hbaseService.createTable(namespace, "test_table_3", "family");

        hbaseService.deleteNamespace(namespace);

        System.out.println("Namespaces: " + hbaseService.getHBaseNamespaces().getNamespaces());
//        System.out.println("Namespaces: " + hBaseMetadataService.getHBaseNamespaces().getNamespaces());
    }

    private FileInputStream getHbaseSiteXmlFileInputStream1() throws FileNotFoundException {
        final String filePath = "/Users/hlouro/Hortonworks/Tasks/IoTaS/ClusterMetadata/ConfigFiles/hbase-site.xml";
        return new FileInputStream(filePath);
    }

    void log1(String msg, String... args) {
        System.out.println(Thread.currentThread().getName() + " - " + Integer.toHexString(System.identityHashCode(this)) + " - " + String.format(msg, args));
    }

    final static String HBASE_TEST_NAMESPACE = "test_namespace";
    final static List<String> HBASE_TEST_TABLES = ImmutableList.copyOf(new String[]{"test_table_1", "test_table_2"});
    final static String HBASE_TEST_TABLE_FAMILY = "test_table_family";

    // ===




    @Test
    public void getHBaseTables() throws Exception {
        try {
            setUp();
            Tables hBaseTables = hbaseService.getHBaseTables();
            System.out.println("Stop");
        } finally {
            tearDown();
        }
    }

    private void setUp() throws Exception {
        new Expectations() {{
            serviceConfiguration.getConfigurationMap(); result = getHBaseConfiProps();
        }};

        hbaseService = HBaseMetadataService.newInstance(catalogService, 1L);

        hbaseService.createNamespace(HBASE_TEST_NAMESPACE);

        for (String table : HBASE_TEST_TABLES) {
            hbaseService.createTable(HBASE_TEST_NAMESPACE, table, HBASE_TEST_TABLE_FAMILY);
        }
    }

    private void tearDown() throws Exception {
        for (String table : HBASE_TEST_TABLES) {
            hbaseService.disableTable(HBASE_TEST_NAMESPACE, table);
            hbaseService.deleteTable(HBASE_TEST_NAMESPACE, table);
        }

        hbaseService.deleteNamespace(HBASE_TEST_NAMESPACE);
    }

    @Test
    public void callteardown() throws Exception {
        new Expectations() {{
            serviceConfiguration.getConfigurationMap(); result = getHBaseConfiProps();
        }};

        hbaseService = HBaseMetadataService.newInstance(catalogService, 1L);
        tearDown();
    }

    /*
        Calling all the tests in one method because table creation during setup is quite expensive and needs to be done in the scope
        of the test because it depends on recorded expectations, which abstract lots of initialization.
     */
    @Test
    public void test_all() throws Exception {
        setUp();
        try {
            test_getHBaseTablesForNamespace();
            test_getHBaseNamespaces();
        } finally {
            tearDown();
        }

    }

    @Test
    public void test_getHBaseTables() throws Exception {
        Tables hBaseTables = hbaseService.getHBaseTables(HBASE_TEST_NAMESPACE);
        Assert.assertEquals(HBASE_TEST_TABLES.stream().map(p -> HBASE_TEST_NAMESPACE + ":" + p).collect(Collectors.toList()),
                hBaseTables.getTables().stream().sorted(String::compareTo).collect(Collectors.toList()));
    }

    @Test
    public void test_getHBaseTablesForNamespace() throws Exception {
        Tables hBaseTables = hbaseService.getHBaseTables(HBASE_TEST_NAMESPACE);
        Assert.assertEquals(HBASE_TEST_TABLES.stream().map(p -> HBASE_TEST_NAMESPACE + ":" + p).collect(Collectors.toList()),
                            hBaseTables.getTables().stream().sorted(String::compareTo).collect(Collectors.toList()));
    }

    @Test
    public void test_getHBaseNamespaces() throws Exception {
        final HBaseMetadataService.Namespaces hBaseNamespaces = hbaseService.getHBaseNamespaces();
        Assert.assertTrue(hBaseNamespaces.getNamespaces().contains(HBASE_TEST_NAMESPACE));
    }

    public static Map<String, String> getHBaseConfiProps() throws IOException {
        final String path = "/Users/hlouro/Hortonworks/Dev/GitHub/hmcl/Streams/streams/catalog/src/test/java/com/hortonworks/iotas/streams/catalog/service/metadata/hbase-config-map.json";
        Map<String, String> config = new ObjectMapper().readValue(new FileInputStream(path), new TypeReference<Map<String, String>>() { });
        return config;
    }
}