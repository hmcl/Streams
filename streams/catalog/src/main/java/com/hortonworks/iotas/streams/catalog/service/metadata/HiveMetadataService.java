package com.hortonworks.iotas.streams.catalog.service.metadata;

import com.hortonworks.iotas.streams.catalog.exception.ServiceConfigurationNotFoundException;
import com.hortonworks.iotas.streams.catalog.exception.ServiceNotFoundException;
import com.hortonworks.iotas.streams.catalog.service.StreamCatalogService;
import com.hortonworks.iotas.streams.catalog.service.metadata.common.OverrideHadoopConfiguration;
import com.hortonworks.iotas.streams.catalog.service.metadata.common.Tables;
import com.hortonworks.iotas.streams.cluster.discovery.ambari.ServiceConfigurations;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Provides Hive databases and database tables metadata information using {@link HiveMetaStoreClient}
 */
public class HiveMetadataService {
    protected static final Logger LOG = LoggerFactory.getLogger(HiveMetadataService.class);

    private static final String STREAMS_JSON_SCHEMA_SERVICE_HIVE = ServiceConfigurations.HIVE.name();
    private static final String STREAMS_JSON_SCHEMA_CONFIG_HIVE_METASTORE_SITE = ServiceConfigurations.HIVE.getConfNames()[3];

    private final HiveConf hiveConf;  // HiveConf used to create HiveMetaStoreClient. If this class is created with the 1 parameter constructor, it is set to null
    private HiveMetaStoreClient metaStoreClient;

    public HiveMetadataService(HiveMetaStoreClient metaStoreClient) {
        this(metaStoreClient, null);
    }

    private HiveMetadataService(HiveMetaStoreClient metaStoreClient, HiveConf hiveConf) {
        this.metaStoreClient = metaStoreClient;
        this.hiveConf = hiveConf;
    }

    /**
     * Creates a new instance of {@link HiveMetadataService} which delegates to {@link HiveMetaStoreClient} with base default
     * {@link HiveConf} and {@code hive-site.xml} config related properties overridden with the values set
     * in the hive metastore hive-site config serialized in "streams services json"
     */
    public static HiveMetadataService newInstance(StreamCatalogService catalogService, Long clusterId)
            throws MetaException, IOException, ServiceConfigurationNotFoundException, ServiceNotFoundException {
        return newInstance(new HiveConf(), catalogService, clusterId);
    }


    /**
     * Creates a new instance of {@link HiveMetadataService} which delegates to {@link HiveMetaStoreClient} the provided
     * {@link HiveConf} as base config, and {@code hive-site.xml} config related properties overridden with the values set
     * in the hive metastore hive-site config serialized in "streams services json"
     */
    public static HiveMetadataService newInstance(HiveConf hiveConf, StreamCatalogService catalogService, Long clusterId)
            throws MetaException, IOException, ServiceConfigurationNotFoundException, ServiceNotFoundException {
        return new HiveMetadataService(new HiveMetaStoreClient(OverrideHadoopConfiguration.override(hiveConf, catalogService,
                ServiceConfigurations.HIVE, clusterId, STREAMS_JSON_SCHEMA_CONFIG_HIVE_METASTORE_SITE)), hiveConf);
    }

    /**
     * @return The table names of for the database specified in the parameter
     */
    public Tables getHiveTables(String dbName) throws MetaException {
        return Tables.newInstance(metaStoreClient.getAllTables(dbName));
    }

    /**
     * @return The names of all databases in the MetaStore.
     */
    public Databases getHiveDatabases() throws MetaException {
        return Databases.newInstance(metaStoreClient.getAllDatabases());
    }

    /**
     * @return The instance of the {@link HiveMetaStoreClient} used to retrieve Hive databases and tables metadata
     */
    public HiveMetaStoreClient getMetaStoreClient() {
        return metaStoreClient;
    }

    /**
     * @return a copy of the {@link HiveConf} used to configure the {@link HiveMetaStoreClient} instance created
     * using the factory methods. null if this object was initialized using the
     * {@link HiveMetadataService#HiveMetadataService(org.apache.hadoop.hive.metastore.HiveMetaStoreClient)} constructor
     */
    public HiveConf getHiveConfCopy() {
        return hiveConf == null ? null : new HiveConf(hiveConf);
    }

    /**
     * Wrapper used to show proper JSON formatting
     */
    public static class Databases {
        private List<String> databases;

        public Databases(List<String> databases) {
            this.databases = databases;
        }

        public static Databases newInstance(List<String> databases) {
            return databases == null ? new Databases(Collections.<String>emptyList()) : new Databases(databases);
        }

        public List<String> getDatabases() {
            return databases;
        }
    }
}
