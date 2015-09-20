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


import com.hortonworks.iotas.catalog.DataFeed;
import com.hortonworks.iotas.catalog.DataSource;
import com.hortonworks.iotas.catalog.Device;
import com.hortonworks.iotas.catalog.ParserInfo;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.service.CatalogService;
import com.hortonworks.iotas.storage.AlreadyExistsException;
import com.hortonworks.iotas.storage.PrimaryKey;
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorableKey;
import com.hortonworks.iotas.storage.StorageException;
import com.hortonworks.iotas.storage.StorageManager;
import com.hortonworks.iotas.storage.impl.jdbc.config.Config;
import com.hortonworks.iotas.storage.impl.jdbc.connection.ConnectionBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlDelete;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlInsertUpdateDuplicate;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlSelect;
import com.hortonworks.iotas.storage.impl.jdbc.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

//TODO: The synchronization is broken right now, so all the methods dont guarantee the semantics as described in the interface.
public class JdbcStorageManager implements StorageManager {
    private static final Logger log = LoggerFactory.getLogger(StorageManager.class);

    protected final List<Connection> activeConnections;
    protected final ConnectionBuilder connectionBuilder;
    protected final int queryTimeoutSecs;
    protected final Config config;

    public JdbcStorageManager(ConnectionBuilder connectionBuilder, Config config) {
        this.connectionBuilder = connectionBuilder;
        this.config = config;
        this.queryTimeoutSecs = config.getQueryTimeoutSecs();
        activeConnections = Collections.synchronizedList(new ArrayList<Connection>());
    }

    @Override
    public void add(Storable storable) throws AlreadyExistsException {
        log.debug("Adding storable [{}]", storable);
        final Storable existing = get(storable.getStorableKey());

        if(existing == null) {
            addOrUpdate(storable);
        } else if (!existing.equals(storable)) {
            throw new AlreadyExistsException("Another instnace with same id = " + storable.getPrimaryKey()
                    + " exists with different value in namespace " + storable.getNameSpace()
                    + " Consider using addOrUpdate method if you always want to overwrite.");
        }
    }

    @Override
    public <T extends Storable> T remove(StorableKey key) throws StorageException {
        T oldVal = get(key);
        if (key != null) {
            Connection connection = null;
            try {
                connection = getConnection();
                PreparedStatement preparedStatement = new MySqlDelete(key).getPreparedStatement(connection, queryTimeoutSecs);
                preparedStatement.executeUpdate();
                preparedStatement.close();
            } catch (SQLException e) {
                throw new StorageException(e);
            } finally {
                closeConnection(connection);
            }
        }
        return oldVal;
    }

    @Override
    public void addOrUpdate(Storable storable) {
        if (storable == null) {
            throw new IllegalArgumentException("Invalid " + Storable.class.getSimpleName() + " : null");
        }
        log.debug("Adding or updating storable [{}]", storable);
        Connection connection = null;
        try {
            connection = getConnection();
            final MySqlInsertUpdateDuplicate sqlBuilder = new MySqlInsertUpdateDuplicate(storable);
            final PreparedStatement preparedStatement = sqlBuilder
                    .getPreparedStatement(connection, queryTimeoutSecs);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new StorageException(e);
        } finally {
            closeConnection(connection);
        }
    }

    @Override
    public <T extends Storable> T get(StorableKey key) throws StorageException {
        Connection connection = null;
        try {
            connection = getConnection();
            PreparedStatement preparedStatement = new MySqlSelect(key).getPreparedStatement(connection, queryTimeoutSecs);
            ResultSet resultSet = preparedStatement.executeQuery();

            final T storable = getStorableFromResultSet(resultSet, key.getNameSpace());

            if (log.isDebugEnabled()) {
                log.debug("Querying key = [{}]\n\t returned Storable = [{}]", key, storable);
            }
            return storable;
        } catch (SQLException e) {
            throw new StorageException(e);
        } finally {
            closeConnection(connection);
        }
    }

    @Override
    public <T extends Storable> List<T> find(String namespace, List<CatalogService.QueryParam> queryParams) throws StorageException {
        if (queryParams == null) {
            return (List<T>) list(namespace);
        }

        log.debug("Finding entries in table [{}] that satisfy queryParams [{}]", namespace, queryParams);
        Connection connection = null;
        try {
            connection = getConnection();
            // Build StorableKey from QueryParam. StorableKey is used to filter the rows in the DB that match the QueryParams
            final StorableKey storableKey = buildStorableKey(namespace, queryParams);
            PreparedStatement preparedStatement = new MySqlSelect(storableKey).getPreparedStatement(connection, queryTimeoutSecs);
            ResultSet resultSet = preparedStatement.executeQuery();

            final Collection<T> storables = getStorablesFromResultSet(resultSet, namespace);

            if (log.isDebugEnabled()) {
                log.debug("Querying table = [{}]\n\t filter = [{}]\n\t returned Storables = [{}]", namespace, queryParams, storables);
            }
            return (List<T>) storables;
        } catch (Exception e) {
            throw new StorageException(e);
        } finally {
            closeConnection(connection);
        }
    }

    @Override
    public <T extends Storable> Collection<T> list(String namespace) throws StorageException {
        Connection connection = null;
        try {
            log.debug("Listing entries for table [{}]", namespace);
            connection = getConnection();
            PreparedStatement preparedStatement = new MySqlSelect(namespace).getPreparedStatement(connection, queryTimeoutSecs);
            ResultSet resultSet = preparedStatement.executeQuery();

            final Collection<T> storables = getStorablesFromResultSet(resultSet, namespace);

            if (log.isDebugEnabled()) {
                log.debug("Querying table = [{}]\n\t returned Storables = [{}]", namespace, storables);
            }

            return storables;
        } catch (SQLException e) {
            throw new StorageException(e);
        } finally {
            closeConnection(connection);
        }
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
        final String sql = "SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = " + namespace;
        Connection connection = null;
        try {
            connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            resultSet.next();
            long nextId = resultSet.getLong("AUTO_INCREMENT");
            log.debug("Next id for auto increment table [{}] = {}", namespace, nextId);
            return nextId;
        } catch (SQLException e) {
            throw new StorageException(e);
        } finally {
            closeConnection(connection);
        }
    }

    // private helper methods

    //TODO: Throw invalid query parameter exception
    private StorableKey buildStorableKey(String namespace, List<CatalogService.QueryParam> queryParams) throws Exception {
        final Map<Schema.Field, Object> fieldsToVal = new HashMap<>();

        for (CatalogService.QueryParam qp : queryParams) {
            final String val = qp.getValue();
            final Schema.Type typeOfVal = Schema.Type.getTypeOfVal(val);
            fieldsToVal.put(new Schema.Field(qp.getName(), typeOfVal),
                            typeOfVal.getJavaType().getConstructor(String.class).newInstance(val));
        }

        final PrimaryKey primaryKey = new PrimaryKey(fieldsToVal);
        final StorableKey storableKey = new StorableKey(namespace, primaryKey);
        log.debug("namespace = [{}]\n\t queryParams = [{}]\n\t StorableKey = [{}]", namespace, queryParams, storableKey);
        return storableKey;
    }

    private <T extends Storable> T newStorableInstance(String nameSpace) {
        switch (nameSpace) {
            case(DataFeed.NAME_SPACE):
                return (T) new DataFeed();
            case(DataSource.NAME_SPACE):
                return (T) new DataSource();
            case(Device.NAME_SPACE):
                return (T) new Device();
            case(ParserInfo.NAME_SPACE):
                return (T) new ParserInfo();
            default:
                throw new RuntimeException("Unsupported Storable type");
        }
    }

    private <T extends Storable> Collection<T> getStorablesFromResultSet(ResultSet resultSet, String nameSpace) {
        final Collection<T> storables = new ArrayList<>();
        // maps contains the data to populate the state of Storable objects
        final List<Map<String, Object>> maps = getMapsFromResultSet(resultSet);
        if (maps != null) {
            for (Map<String, Object> map : maps) {
                if (map != null) {
                    T storable = newStorableInstance(nameSpace);
                    storable.fromMap(map);      // populates the Storable object state
                    storables.add(storable);
                }
            }
        }
        return storables;
    }

    private <T extends Storable> T getStorableFromResultSet(ResultSet resultSet, String nameSpace) {
        T storable = null;
        Map<String, Object> mapFromResultSet = getMapFromResultSet(resultSet);
        if (mapFromResultSet != null) {
            storable = newStorableInstance(nameSpace);
            storable.fromMap(mapFromResultSet);
        }
        return storable;
    }

    // returns null for empty ResultSet or ResultSet with no rows
    private List<Map<String, Object>> getMapsFromResultSet(ResultSet resultSet) {
        List<Map<String, Object>> maps = null;

        try {
            if (resultSet.first()) {    // returns false if no rows in result set. Otherwise points to first row
                maps = new LinkedList<>();
                ResultSetMetaData rsMetadata = resultSet.getMetaData();
                Map<String, Object> map = newMapWithRowContents(resultSet, rsMetadata);;
                maps.add(map);

                while (resultSet.next()) {
                    map = newMapWithRowContents(resultSet, rsMetadata);;
                    maps.add(map);
                }
            }
        } catch (SQLException e) {
            log.error("Exception occurred while processing result set.", e);
        }
        return maps;
    }

    private <T extends Storable> Map<String, Object> getMapFromResultSet(ResultSet resultSet) {
        Map<String, Object> map = null;
        try {
            if (resultSet.first()) {    // returns false if no rows in result set. Otherwise points to first row
                map = newMapWithRowContents(resultSet, resultSet.getMetaData());
            }
        } catch (SQLException e) {
            log.error("Exception occurred while processing ResultSet.", e);
        }
        return map;
    }

    private Map<String, Object> newMapWithRowContents(ResultSet resultSet, ResultSetMetaData rsMetadata) throws SQLException {
        final Map<String, Object> map = new HashMap<>();
        final int columnCount = rsMetadata.getColumnCount();

        for (int i = 1 ; i <= columnCount; i++) {
            final String columnLabel = rsMetadata.getColumnLabel(i);
            final int columnType = rsMetadata.getColumnType(i);
            final Class columnJavaType = Util.getJavaType(columnType);

            if (columnJavaType.equals(String.class)) {
                map.put(columnLabel, resultSet.getString(columnLabel));
            } else if (columnJavaType.equals(Integer.class)) {
                map.put(columnLabel, resultSet.getInt(columnLabel));
            } else if (columnJavaType.equals(Double.class)) {
                map.put(columnLabel, resultSet.getDouble(columnLabel));
            } else if (columnJavaType.equals(Float.class)) {
                map.put(columnLabel, resultSet.getFloat(columnLabel));
            } else if (columnJavaType.equals(Short.class)) {
                map.put(columnLabel, resultSet.getShort(columnLabel));
            } else if (columnJavaType.equals(Boolean.class)) {
                map.put(columnLabel, resultSet.getBoolean(columnLabel));
            } else if (columnJavaType.equals(byte[].class)) {
                map.put(columnLabel, resultSet.getBytes(columnLabel));
            } else if (columnJavaType.equals(Long.class)) {
                map.put(columnLabel, resultSet.getLong(columnLabel));
            } else if (columnJavaType.equals(Date.class)) {
                map.put(columnLabel, resultSet.getDate(columnLabel));
            } else if (columnJavaType.equals(Time.class)) {
                map.put(columnLabel, resultSet.getTime(columnLabel));
            } else if (columnJavaType.equals(Timestamp.class)) {
                map.put(columnLabel, resultSet.getTimestamp(columnLabel));
            } else {
                throw new StorageException("type =  [" + columnType + "] for column [" + columnLabel + "] not supported.");
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Row for ResultSet [{}] with metadata [{}] generated Map [{}]", resultSet, rsMetadata, map);
        }
        return map;
    }

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
}
