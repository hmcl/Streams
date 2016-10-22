package com.hortonworks.iotas.streams.catalog.service.metadata;

import com.google.common.collect.ImmutableList;

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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import mockit.Expectations;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;

@RunWith(JMockit.class)
public class HBaseMetadataServiceTest {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseMetadataServiceTest.class);

    private final static String HBASE_TEST_NAMESPACE = "test_namespace";
    private final static List<String> HBASE_TEST_TABLES = ImmutableList.copyOf(new String[]{"test_table_1", "test_table_2"});
    private final static String HBASE_TEST_TABLE_FAMILY = "test_table_family";

    private HBaseMetadataService hbaseService;

    @Mocked
    private StreamCatalogService catalogService;
    @Mocked
    private ServiceConfiguration serviceConfiguration;

    private void setUp() throws Exception {
        new Expectations() {{
            serviceConfiguration.getConfigurationMap(); result = getHBaseConfiProps1();
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

    /*
        Calling all the tests in one method because table creation during setup is quite expensive and needs to be done
        in the scope of the test because it depends on recorded expectations, which abstract lots of initialization.
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

    public void test_getHBaseTables() throws Exception {
        Tables hBaseTables = hbaseService.getHBaseTables();
        Assert.assertEquals(HBASE_TEST_TABLES.stream().map(p -> HBASE_TEST_NAMESPACE + ":" + p).collect(Collectors.toList()),
                hBaseTables.getTables().stream().sorted(String::compareTo).collect(Collectors.toList()));
    }

    private void test_getHBaseTablesForNamespace() throws Exception {
        Tables hBaseTables = hbaseService.getHBaseTables(HBASE_TEST_NAMESPACE);
        Assert.assertEquals(HBASE_TEST_TABLES.stream().map(p -> HBASE_TEST_NAMESPACE + ":" + p).collect(Collectors.toList()),
                            hBaseTables.getTables().stream().sorted(String::compareTo).collect(Collectors.toList()));
    }

    private void test_getHBaseNamespaces() throws Exception {
        final HBaseMetadataService.Namespaces hBaseNamespaces = hbaseService.getHBaseNamespaces();
        Assert.assertTrue(hBaseNamespaces.getNamespaces().contains(HBASE_TEST_NAMESPACE));
    }

    private Map<String, String> getHBaseConfiProps1() throws IOException {
        Map<String, String> config = new ObjectMapper().readValue(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("metadata/hivemetastore-site.json"),
                new TypeReference<Map<String, String>>() { });
        return config;
    }
}