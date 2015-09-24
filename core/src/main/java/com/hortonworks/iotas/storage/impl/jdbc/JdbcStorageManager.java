/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.iotas.storage.impl.jdbc;


import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.service.CatalogService;
import com.hortonworks.iotas.storage.AlreadyExistsException;
import com.hortonworks.iotas.storage.PrimaryKey;
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorableKey;
import com.hortonworks.iotas.storage.StorageException;
import com.hortonworks.iotas.storage.StorageManager;
import com.hortonworks.iotas.storage.exception.IllegalQueryParameterException;
import com.hortonworks.iotas.storage.impl.jdbc.connection.ConnectionBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.factory.SqlExecutor;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MetadataHelper;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlSelect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//TODO: The synchronization is broken right now, so all the methods dont guarantee the semantics as described in the interface.
public class JdbcStorageManager implements StorageManager {
    private static final Logger log = LoggerFactory.getLogger(StorageManager.class);

    protected final List<Connection> activeConnections;
    protected final ConnectionBuilder connectionBuilder;
    protected final SqlExecutor sqlExecutor;

    public JdbcStorageManager(ConnectionBuilder connectionBuilder, SqlExecutor sqlExecutor) {
        this.connectionBuilder = connectionBuilder;
        this.sqlExecutor = sqlExecutor;
        activeConnections = Collections.synchronizedList(new ArrayList<Connection>());
    }

    @Override
    public void add(Storable storable) throws AlreadyExistsException {
        log.debug("Adding storable [{}]", storable);
        final Storable existing = get(storable.getStorableKey());

        if(existing == null) {
            addOrUpdate(storable);
        } else if (!existing.equals(storable)) {
            throw new AlreadyExistsException("Another instance with same id = " + storable.getPrimaryKey()
                    + " exists with different value in namespace " + storable.getNameSpace()
                    + ". Consider using addOrUpdate method if you always want to overwrite.");
        }
    }

    @Override
    public <T extends Storable> T remove(StorableKey key) throws StorageException {
        T oldVal = get(key);
        if (key != null) {
            log.debug("Removing storable key [{}]", key);
            sqlExecutor.delete(key);
        }
        return oldVal;
    }

    @Override
    public void addOrUpdate(Storable storable) throws StorageException {
        if (storable == null) {
            throw new IllegalArgumentException("Invalid " + Storable.class.getSimpleName() + " : null");
        }
        log.debug("Adding or updating storable [{}]", storable);
        sqlExecutor.insertOrUpdate(storable);
    }

    @Override
    public <T extends Storable> T get(StorableKey key) throws StorageException {
        log.debug("Removing storable key [{}]", key);

        final Collection<T> entries = sqlExecutor.select(key);
        if (entries.size() > 1) {
            log.debug("More than one entry found for storable key [{}]", key);
        }
        final T entry = entries.iterator().next();
        log.debug("Querying key = [{}]\n\t returned [{}]", key, entry);
        return entry;
    }

    @Override
    public <T extends Storable> Collection<T> find(String namespace, List<CatalogService.QueryParam> queryParams) throws StorageException {
        log.debug("Finding entries in table [{}] that match queryParams [{}]", namespace, queryParams);

        if (queryParams == null) {
            return list(namespace);
        }

        Connection connection = null;
        try {
            // Build StorableKey from QueryParam. StorableKey is used to filter the rows in the DB that match the QueryParams
            Collection<T> entries = Collections.EMPTY_LIST;

            StorableKey storableKey = buildStorableKey(namespace, queryParams);
            if (storableKey != null) {
                entries = sqlExecutor.select(storableKey);
            }

            if (log.isDebugEnabled()) {
                log.debug("Querying table = [{}]\n\t filter = [{}]\n\t returned [{}]", namespace, queryParams, entries);
            }
            return entries;
        } catch (Exception e) {
            throw new StorageException(e);
        } finally {
            closeConnection(connection);
        }
    }

    @Override
    public <T extends Storable> Collection<T> list(String namespace) throws StorageException {
        log.debug("Listing entries for table [{}]", namespace);
        final Collection<T> entries = sqlExecutor.select(namespace);
        log.debug("Querying table = [{}]\n\t returned [{}]", namespace, entries);
        return entries;
    }

    @Override
    public void cleanup() throws StorageException {
        try {
            closeAllActiveConnections();
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    private void closeAllActiveConnections() throws SQLException {
        for (Connection activeConnection : activeConnections) {
            if (!activeConnection.isClosed()) {
                activeConnection.close();
            }
        }
    }

    @Override
    public Long nextId(String namespace) {
        // This only works if the table has auto-increment. The TABLE_SCHEMA part is implicitly specified in the Connection object
        // SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'temp' AND TABLE_SCHEMA = 'test'
        Connection connection = null;
        try {
            connection = getConnection();
            return getNextId(connection, namespace);
        } catch (SQLException e) {
            throw new StorageException(e);
        } finally {
            closeConnection(connection);
        }
    }

    // Package protected to be able to override it in the test framework
    Long getNextId(Connection connection, String namespace) throws SQLException {
        return MetadataHelper.nextIdMySql(connection, namespace, queryTimeoutSecs);
    }

    // private helper methods

    Connection getConnection() throws SQLException {
        Connection connection = connectionBuilder.getConnection();
        activeConnections.add(connection);
        return connection;
    }

    void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
                activeConnections.remove(connection);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to close connection", e);
            }
        }
    }

    /**
     * @return null if none of the none of the query parameters specified matches a column in the DB
     */
    private MySqlBuilder getMySqlBuilderForQueryParams(String namespace, List<CatalogService.QueryParam> queryParams) throws Exception {
        final StorableKey storableKey = buildStorableKey(namespace, queryParams);
        return storableKey == null ?
                null :
                new MySqlSelect(storableKey);
    }

    /**
     * Query parameters are typically specified for a column or key in a database table or storage namespace. Therefore, we build
     * the {@link StorableKey} from the list of query parameters, and then can use {@link MySqlSelect} builder to generate the query using
     * the query parameters in the where clause
     *
     * @return {@link StorableKey} with all query parameters that match database columns <br/>
     * null if none of the none of the query parameters specified matches a column in the DB
     */
    private StorableKey buildStorableKey(String namespace, List<CatalogService.QueryParam> queryParams) throws Exception {
        final Map<Schema.Field, Object> fieldsToVal = new HashMap<>();
        StorableKey storableKey = null;

        try {
            for (CatalogService.QueryParam qp : queryParams) {
                if (!MetadataHelper.isColumnInNamespace(getConnection(), queryTimeoutSecs, namespace, qp.getName())) {
                    log.warn("Query parameter [{}] does not exist for namespace [{}]. Query parameter ignored.", qp.getName(), namespace);
                } else {
                    final String val = qp.getValue();
                    final Schema.Type typeOfVal = Schema.Type.getTypeOfVal(val);
                    fieldsToVal.put(new Schema.Field(qp.getName(), typeOfVal),
                            typeOfVal.getJavaType().getConstructor(String.class).newInstance(val)); // instantiates object of the appropriate type
                }
            }

            // it is empty when none of the query parameters specified matches a column in the DB
            if (!fieldsToVal.isEmpty()) {
                final PrimaryKey primaryKey = new PrimaryKey(fieldsToVal);
                storableKey = new StorableKey(namespace, primaryKey);
            }

            log.debug("Building StorableKey from QueryParam: \n\tnamespace = [{}]\n\t queryParams = [{}]\n\t StorableKey = [{}]", namespace, queryParams, storableKey);
        } catch (Exception e) {
            log.debug("Exception occurred when attempting to generate StorableKey from QueryParam", e);
            throw new IllegalQueryParameterException(e);
        }

        return storableKey;
    }
}
