package com.hortonworks.iotas.streams.catalog.service.metadata;

import com.hortonworks.iotas.streams.catalog.exception.ServiceConfigurationNotFoundException;
import com.hortonworks.iotas.streams.catalog.exception.ServiceNotFoundException;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;
import com.hortonworks.iotas.streams.catalog.service.metadata.common.OverrideHadoopConfiguration;
import com.hortonworks.iotas.streams.catalog.service.metadata.common.Tables;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.ServiceConfigurations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Provides HBase databases tables metadata information using {@link org.apache.hadoop.hbase.client.HBaseAdmin}
 */
public class HBaseMetadataService {
    private static final String STREAMS_JSON_SCHEMA_SERVICE_HBASE = ServiceConfigurations.HBASE.name();
    private static final String STREAMS_JSON_SCHEMA_CONFIG_HBASE_SITE = ServiceConfigurations.HBASE.getConfNames()[2];

    private Admin hBaseAdmin;

    public HBaseMetadataService(Admin hBaseAdmin) {
        this.hBaseAdmin = hBaseAdmin;
    }

    public static HBaseMetadataService newInstance(StreamCatalogService catalogService, Long clusterId)
            throws IOException, ServiceConfigurationNotFoundException, ServiceNotFoundException {

        return newInstance(HBaseConfiguration.create(), catalogService, clusterId);
    }

    public static HBaseMetadataService newInstance(Configuration hbaseConfig, StreamCatalogService catalogService, Long clusterId)
            throws IOException, ServiceConfigurationNotFoundException, ServiceNotFoundException {

        return new HBaseMetadataService(ConnectionFactory.createConnection(
                OverrideHadoopConfiguration.override(hbaseConfig, catalogService,
                        ServiceConfigurations.HBASE, clusterId, STREAMS_JSON_SCHEMA_CONFIG_HBASE_SITE))
                .getAdmin());
    }

    /**
     * @return All tables for all namespaces
     */
    public Tables getHBaseTables() throws IOException {
        final TableName[] tableNames = hBaseAdmin.listTableNames();
        return Tables.newInstance(tableNames);
    }

    /**
     * @param namespace Namespace for which to get table names
     * @return All tables for the namespace given as parameter
     */
    public Tables getHBaseTables(String namespace) throws IOException {
        final TableName[] tableNames = hBaseAdmin.listTableNamesByNamespace(namespace);
        return Tables.newInstance(tableNames);
    }

    /**
     * @return All namespaces
     */
    public Namespaces getHBaseNamespaces() throws IOException {
        return Namespaces.newInstance(hBaseAdmin.listNamespaceDescriptors());
    }

    /**
     * Wrapper used to show proper JSON formatting
     */
    public static class Namespaces {
        private List<String> namespaces;

        public Namespaces(List<String> namespaces) {
            this.namespaces = namespaces;
        }

        public static Namespaces newInstance(NamespaceDescriptor[] namespaceDescriptors) {
            List<String> namespaces = Collections.emptyList();
            if (namespaceDescriptors != null) {
                namespaces = new ArrayList<>(namespaceDescriptors.length);
                for (NamespaceDescriptor namespace : namespaceDescriptors) {
                    namespaces.add(namespace.getName());
                }
            }
            return new Namespaces(namespaces);
        }

        public List<String> getNamespaces() {
            return namespaces;
        }
    }
}
