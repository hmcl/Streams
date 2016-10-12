package com.hortonworks.iotas.streams.catalog.service.metadata;

import com.hortonworks.iotas.streams.catalog.exception.ServiceConfigurationNotFoundException;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;
import com.hortonworks.iotas.streams.catalog.service.metadata.common.OverrideHadoopConfiguration;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.ServiceConfigurations;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;
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
            throws IOException, ServiceConfigurationNotFoundException {

        return new HBaseMetadataService(ConnectionFactory.createConnection(
                OverrideHadoopConfiguration.override(HBaseConfiguration.create(), catalogService,
                        STREAMS_JSON_SCHEMA_SERVICE_HBASE, clusterId, STREAMS_JSON_SCHEMA_CONFIG_HBASE_SITE))
                .getAdmin());
    }

    public List<String> getHBaseTables() throws IOException {
        final TableName[] tableNames = hBaseAdmin.listTableNames();
        return getTableNamesAsString(tableNames);
    }

    public List<String> getHBaseTables(String namespace) throws IOException {
        final TableName[] tableNames = hBaseAdmin.listTableNamesByNamespace(namespace);
        return getTableNamesAsString(tableNames);
    }

    private List<String> getTableNamesAsString(TableName[] tableNames) {
        List<String> fqTableNames = null;
        if (tableNames != null) {
            fqTableNames = new ArrayList<>(tableNames.length);
            for (TableName tableName : tableNames) {
                fqTableNames.add(tableName.getNameWithNamespaceInclAsString());
            }
        }
        return fqTableNames;
    }

    public List<String> getHBaseNamespaces() throws IOException {
        final NamespaceDescriptor[] namespaceDescs = hBaseAdmin.listNamespaceDescriptors();
        List<String> namespaces = null;
        if (namespaceDescs != null) {
            namespaces = new ArrayList<>(namespaceDescs.length);
            for (NamespaceDescriptor namespace : namespaceDescs) {
                namespaces.add(namespace.getName());
            }
        }
        return namespaces;
    }
}
