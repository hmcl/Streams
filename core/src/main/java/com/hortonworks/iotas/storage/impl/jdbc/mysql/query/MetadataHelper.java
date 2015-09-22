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

package com.hortonworks.iotas.storage.impl.jdbc.mysql.query;

import com.hortonworks.iotas.storage.exception.NonIncrementalKeyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class MetadataHelper {
    private static final Logger log = LoggerFactory.getLogger(MetadataHelper.class);

    public static boolean isAutoIncrement(Connection connection, String namespace, int queryTimeoutSecs) throws SQLException {
        final ResultSetMetaData rsMetadata = new MySqlSelect(namespace)
                .getPreparedStatement(connection, queryTimeoutSecs).executeQuery().getMetaData();
        final int columnCount = rsMetadata.getColumnCount();

        for (int i = 1 ; i <= columnCount; i++) {
            if (rsMetadata.isAutoIncrement(i)) {
                return true;
            }
        }
        return false;
    }

    public static long nextId(Connection connection, String namespace, int queryTimeoutSecs) throws SQLException {
        if (!isAutoIncrement(connection, namespace, queryTimeoutSecs)) {
            throw new NonIncrementalKeyException();
        }

        final ResultSet resultSet = new MySqlQuery(buildNextIdSql(connection, namespace))
                .getPreparedStatement(connection, queryTimeoutSecs).executeQuery();
        resultSet.next();
        final long nextId = resultSet.getLong("AUTO_INCREMENT");
        log.debug("Next id for auto increment table [{}] = {}", namespace, nextId);
        return nextId;
    }

    protected static String buildNextIdSql(Connection connection, String namespace) throws SQLException {
        final String database  = connection.getCatalog();
        final String sql = "SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '"
                + namespace + "' AND TABLE_SCHEMA = '" + database + "'";
        log.debug("nextId() SQL query: {}", sql);
        return sql;
    }

    public static boolean isColumnInNamespace(Connection connection, int queryTimeoutSecs, String namespace, String columnName) throws SQLException {
        final ResultSetMetaData rsMetadata = new MySqlSelect(namespace)
                .getPreparedStatement(connection, queryTimeoutSecs).executeQuery().getMetaData();
        final int columnCount = rsMetadata.getColumnCount();

        for (int i = 1 ; i <= columnCount; i++) {
            if (rsMetadata.getColumnName(i).equalsIgnoreCase(columnName)) {
                return true;
            }
        }
        return false;
    }

}
