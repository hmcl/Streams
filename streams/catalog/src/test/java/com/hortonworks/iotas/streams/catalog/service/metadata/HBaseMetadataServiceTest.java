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
import java.util.stream.Stream;

import mockit.Expectations;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;

@RunWith(JMockit.class)
public class HBaseMetadataServiceTest {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseMetadataServiceTest.class);

    private final static List<String> HBASE_TEST_NAMESPACES = ImmutableList.copyOf(new String[]{"test_namespace_1", "test_namespace_2"});
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

        for (String namespace : HBASE_TEST_NAMESPACES) {
            hbaseService.createNamespace(namespace);
            for (String table : HBASE_TEST_TABLES) {
                hbaseService.createTable(namespace, table, HBASE_TEST_TABLE_FAMILY);
            }
        }
    }

    private void tearDown() throws Exception {
        for (String namespace : HBASE_TEST_NAMESPACES) {
            for (String table : HBASE_TEST_TABLES) {
                hbaseService.disableTable(namespace, table);
                hbaseService.deleteTable(namespace, table);
            }
            hbaseService.deleteNamespace(namespace);
        }
    }

    /*
        Calling all the tests in one method because table creation during setup is quite expensive and needs to be done
        in the scope of the test because it depends on recorded expectations in order to abstract lots of initialization.
     */
    @Test
    public void test_all() throws Exception {
        setUp();
        try {
            test_getHBaseTables();
//            test_getHBaseTablesForNamespace();
//            test_getHBaseNamespaces();
        } finally {
            tearDown();
        }
    }
//        "A B C D"
//        "1 2 3 4"

    private void test_getHBaseTables() throws Exception {
        Tables hBaseTables = hbaseService.getHBaseTables();
        /*Assert.assertEquals(HBASE_TEST_NAMESPACES.stream().forEach(ns -> HBASE_TEST_TABLES.stream().map(stream -> ns + ":" + stream).collect(Collectors.toList())),
                hBaseTables.getTables().stream().sorted(String::compareTo).collect(Collectors.toList()));*/

        /*Assert.assertEquals(HBASE_TEST_NAMESPACES.stream().map(ns -> ns + ":" + HBASE_TEST_TABLES.stream().forEach(stream -> ns + ":" + stream).collect(Collectors.toList())),
                hBaseTables.getTables().stream().sorted(String::compareTo).collect(Collectors.toList()));
        */

//        Stream.concat(HBASE_TEST_NAMESPACES.stream(), HBASE_TEST_TABLES.stream()).map((ns, st) -> ns + ":" + st.toString()).collect(Collectors.toList());

        // HBASE_TEST_TABLES.stream().forEach(st -> System.out.println(ns + ":" + st))
        Assert.assertEquals(HBASE_TEST_NAMESPACES.stream()
//                .flatMap(ns -> Stream.of(ns.split("")))
                .flatMap(ns -> HBASE_TEST_TABLES.stream().map(st -> ns + ":" + st))
                .collect(Collectors.toList()),
                hBaseTables.getTables().stream().sorted(String::compareTo).collect(Collectors.toList()));


        /*Assert.assertEquals(HBASE_TEST_NAMESPACES.stream().map(HBASE_TEST_NAMESPACES.stream().peek())HBASE_TEST_NAMESPACES.stream().map(ns -> ns + ":" + HBASE_TEST_TABLES.stream().forEach(stream -> ns + ":" + stream).collect(Collectors.toList())),
                hBaseTables.getTables().stream().sorted(String::compareTo).collect(Collectors.toList()));*/
    }

    private void test_getHBaseTablesForNamespace() throws Exception {
        Tables hBaseTables = hbaseService.getHBaseTables(HBASE_TEST_NAMESPACES.get(0));
        Assert.assertEquals(HBASE_TEST_TABLES.stream().map(p -> HBASE_TEST_NAMESPACES.get(0) + ":" + p).collect(Collectors.toList()),
                            hBaseTables.getTables().stream().sorted(String::compareTo).collect(Collectors.toList()));
    }

    private void test_getHBaseNamespaces() throws Exception {
        final HBaseMetadataService.Namespaces hBaseNamespaces = hbaseService.getHBaseNamespaces();
        Assert.assertTrue(hBaseNamespaces.getNamespaces().containsAll(HBASE_TEST_NAMESPACES));
    }

    private Map<String, String> getHBaseConfiProps1() throws IOException {
        Map<String, String> config = new ObjectMapper().readValue(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("metadata/hivemetastore-site.json"),
                new TypeReference<Map<String, String>>() { });
        return config;
    }
}