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

package com.hortonworks.iotas.storage.impl.jdbc;

import com.hortonworks.IntegrationTest;
import com.hortonworks.iotas.storage.AbstractStoreManagerTest;
import com.hortonworks.iotas.storage.StorageManager;
import com.hortonworks.iotas.storage.impl.jdbc.connection.ConnectionBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.connection.HikariCPConnectionBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlBuilder;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Category(IntegrationTest.class)
public abstract class JdbcIntegrationTest extends AbstractStoreManagerTest{
    protected static final Logger log = LoggerFactory.getLogger(JdbcIntegrationTest.class);
    protected static ConnectionBuilder connectionBuilder;

    @BeforeClass
    public static void setUpClass() throws Exception {
        log.debug("JdbcIntegrationTest");
        setConnectionBuilder();
    }

    protected static void setConnectionBuilder() {
        Map<String, Object> config = getMySqlHikariConfig();
//        Map<String, Object> config = getH2HikariConfig();
        connectionBuilder = new HikariCPConnectionBuilder(config);
    }

    // Hikari config to connect to MySql databases
    protected static Map<String, Object> getMySqlHikariConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        config.put("dataSource.url", "jdbc:mysql://localhost/test");
        config.put("dataSource.user", "root");
        return config;
    }

    // Hikari config to connect to H2 databases. Useful for integration tests
    protected static Map<String, Object> getH2HikariConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("dataSourceClassName", "org.h2.jdbcx.JdbcDataSource");
//        In memory configuration. Faster, useful for integration tests
        config.put("dataSource.URL", "jdbc:h2:mem:test;MODE=MySQL;DATABASE_TO_UPPER=false");
//        Embedded configuration. Facilitates debugging by allowing connecting to DB and querying tables
//        config.put("dataSource.URL", "jdbc:h2:~/test;MODE=MySQL;DATABASE_TO_UPPER=false");
        return config;
    }

    @Override
    protected abstract StorageManager getStorageManager() ;

    protected Connection getConnection() {
        return connectionBuilder.getConnection();
    }

    protected void executeSql(final String sql) throws SQLException {
        new MySqlBuilder() {
            @Override
            public String getParameterizedSql() {
                return sql;
            }

            @Override
            public PreparedStatement getPreparedStatement(Connection connection, int queryTimeoutSecs) throws SQLException {
                return this.prepareStatement(connection, queryTimeoutSecs);
            }
        }.getPreparedStatement(connectionBuilder.getConnection(), -1).execute();
    }
}
