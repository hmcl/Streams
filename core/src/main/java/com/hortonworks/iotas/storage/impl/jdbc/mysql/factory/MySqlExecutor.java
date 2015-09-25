/*
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

package com.hortonworks.iotas.storage.impl.jdbc.mysql.factory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.hortonworks.iotas.catalog.DataFeed;
import com.hortonworks.iotas.catalog.DataSource;
import com.hortonworks.iotas.catalog.Device;
import com.hortonworks.iotas.catalog.ParserInfo;
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorableKey;
import com.hortonworks.iotas.storage.StorageException;
import com.hortonworks.iotas.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.iotas.storage.impl.jdbc.connection.ConnectionBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MetadataHelper;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlDelete;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlInsert;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlInsertUpdateDuplicate;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlSelect;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.SqlBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.statement.PreparedStatementBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.util.Util;

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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class MySqlExecutor implements SqlExecutor {
    private Cache<SqlBuilder, PreparedStatementBuilder> cache;
    private final ExecutionConfig config;
    private final int queryTimeoutSecs;
    private final ConnectionBuilder connectionBuilder;
    private final List<Connection> activeConnections;

    public MySqlExecutor(CacheBuilder<SqlBuilder, PreparedStatementBuilder> cacheBuilder,
                         ExecutionConfig config, ConnectionBuilder connectionBuilder) {
        this.connectionBuilder = connectionBuilder;
        this.config = config;
        this.queryTimeoutSecs = config.getQueryTimeoutSecs();
        activeConnections = Collections.synchronizedList(new ArrayList<Connection>());
        setCache(cacheBuilder);
    }

    protected void setCache(CacheBuilder<SqlBuilder, PreparedStatementBuilder> cacheBuilder) {
        cache = cacheBuilder.removalListener(new RemovalListener<SqlBuilder, PreparedStatementBuilder>() {
            /** Removes database connection when the entry is removed from cache*/
            @Override
            public void onRemoval(RemovalNotification<SqlBuilder, PreparedStatementBuilder> notification) {
                final PreparedStatementBuilder val = notification.getValue();
                log.debug("Removing entry from cache and closing connection [key:{}, val: {}]", notification.getKey(), val);
                if (val != null) {
                    closeConnection(val.getConnection());;
                }
            }
        }).build();
    }

    public Cache<SqlBuilder, PreparedStatementBuilder> getCache() {
        return cache;
    }

    public void printCacheState() {
        long size = cache.size();
        CacheStats stats = cache.stats();
        log.debug("size = " + size);
        log.debug("stats = " + stats);
    }

    public void printActiveConnections() {
        log.debug("ACTIVE CONNECTIONS: ", activeConnections);
    }

    // ============= Public API methods =============

    @Override
    public void insert(Storable storable) {
        executeUpdate(storable.getNameSpace(), new MySqlInsert(storable));
    }

    @Override
    public void insertOrUpdate(final Storable storable) {
        executeUpdate(storable.getNameSpace(), new MySqlInsertUpdateDuplicate(storable));
    }

    @Override
    public void delete(StorableKey storableKey) {
        executeUpdate(storableKey.getNameSpace(), new MySqlDelete(storableKey));
    }

    @Override
    public <T extends Storable> Collection<T> select(final String namespace) {
        return executeQuery(namespace, new MySqlSelect(namespace));
    }

    @Override
    public <T extends Storable> Collection<T> select(final StorableKey storableKey){
        return executeQuery(storableKey.getNameSpace(), new MySqlSelect(storableKey));
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

    @Override
    public Connection getConnection() {
        Connection connection = connectionBuilder.getConnection();
//        log.debug("OPENED CONNECTION {}", connection);
        activeConnections.add(connection);
        return connection;
    }

    public void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
//                log.debug("CLOSED CONNECTION {}", connection);
                activeConnections.remove(connection);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to close connection", e);
            }
        }
    }

    public void cleanup() {
        cache.invalidateAll();
    }

    public ExecutionConfig getConfig() {
        return config;
    }

    // =============== Private helper Methods ===============

    private class PreparedStatementBuilderCallable implements Callable<PreparedStatementBuilder> {
        private final SqlBuilder sqlBuilder;

        public PreparedStatementBuilderCallable(SqlBuilder sqlBuilder) {
            this.sqlBuilder = sqlBuilder;
        }

        @Override
        public PreparedStatementBuilder call() throws Exception {
            final PreparedStatementBuilder preparedStatementBuilder = new PreparedStatementBuilder(getConnection(), config, sqlBuilder);
            log.debug("Loading cache with [{}]", preparedStatementBuilder);
            return preparedStatementBuilder;
        }
    }

    private void executeUpdate(String namespace, SqlBuilder sqlBuilder) {
        try {
            final PreparedStatement preparedStatement = getPreparedStatement(sqlBuilder, namespace);
            preparedStatement.executeUpdate();
        } catch (ExecutionException | SQLException e) {
            throw new StorageException(e);
        }
    }


    private <T extends Storable> Collection<T> executeQuery(String namespace, SqlBuilder sqlBuilder) {
        Collection<T> result;
        try {
            final PreparedStatement preparedStatement = getPreparedStatement(sqlBuilder, namespace);
            ResultSet resultSet = preparedStatement.executeQuery();
            result = getStorablesFromResultSet(resultSet, namespace);
        } catch (ExecutionException | SQLException e) {
            throw new StorageException(e);
        }
        return result;
    }

    private PreparedStatement getPreparedStatement(SqlBuilder sqlBuilder, String namespace)
            throws ExecutionException, SQLException {
        PreparedStatementBuilder preparedStatementBuilder =
                cache.get(sqlBuilder, new PreparedStatementBuilderCallable(sqlBuilder));
        return preparedStatementBuilder.getPreparedStatement(sqlBuilder);
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
}
