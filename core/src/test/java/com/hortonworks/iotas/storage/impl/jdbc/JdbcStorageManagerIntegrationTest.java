/*
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
import com.hortonworks.iotas.storage.impl.jdbc.config.Config;
import com.hortonworks.iotas.storage.impl.jdbc.config.HikariBasicConfig;
import com.hortonworks.iotas.storage.impl.jdbc.connection.ConnectionBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.connection.HikariCPConnectionBuilder;
import org.h2.tools.RunScript;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

@Category(IntegrationTest.class)
public class JdbcStorageManagerIntegrationTest extends AbstractStoreManagerTest {
    private static StorageManager jdbcStorageManager;

    // ===== Test Setup ====

    @BeforeClass
    public static void setUpClass() throws Exception {
        // Connection has autoCommit set to false in order to allow rolling back transactions
//        setJdbcStorageManager(new HikariCPConnectionBuilder(HikariBasicConfig.getMySqlHikariTestConfig()));
        setJdbcStorageManager(new HikariCPConnectionBuilder(HikariBasicConfig.getH2HikariTestConfig()));
    }

    @Before
    public void setUp() throws Exception {
        createTables();
    }

    @After
    public void tearDown() throws Exception {
        dropTables();
    }

    private static void setJdbcStorageManager(ConnectionBuilder connectionBuilder) {
        jdbcStorageManager = new JdbcStorageManagerForTest(connectionBuilder, new Config(-1));
    }


    @Override
    protected StorageManager getStorageManager() {
        return jdbcStorageManager;
    }

    //Device has foreign key in DataSource table, which has to be initialized before we can insert data in the Device table
    class DeviceJdbcTest extends DeviceTest {
        @Override
        public void init() {
            addStorables(new DataSourceTest().getStorableList());
        }
    }

    // DataFeed has foreign keys in ParserInfo and DataSource tables, which have to be
    // initialized before we can insert data in the DataFeed table
    class DataFeedsJdbcTest extends DataFeedsTest {
        @Override
        public void init() {
            addStorables(new ParsersTest().getStorableList());
            addStorables(new DataSourceTest().getStorableList());
        }
    }

    protected Connection getConnection() {
        try {
            return ((JdbcStorageManagerForTest)jdbcStorageManager).getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("Cannot get connection using JdbcStorageManagerForTest", e);
        }
    }

    /**
     * This class overrides the connection methods to allow the tests to rollback the transactions and thus not commit to DB
     */
    private static class JdbcStorageManagerForTest extends JdbcStorageManager {
        public JdbcStorageManagerForTest(ConnectionBuilder connectionBuilder, Config config) {
            super(connectionBuilder, config);
        }

        private Connection connection;

        @Override
        protected Connection getConnection() throws SQLException {
            if (connection == null || connection.isClosed()) {
                setConnection();
            }
            return connection;
        }

        private void setConnection() throws SQLException {
            Connection connection = connectionBuilder.getConnection();
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            this.connection = connection;
            activeConnections.add(connection);
        }

        @Override
        protected void closeConnection(Connection connection) {
            // Do not close the connection
        }
    }

    private void createTables() throws SQLException, IOException {
        RunScript.execute(getConnection(), load("create_tables.sql"));
    }

    private void dropTables() throws SQLException, IOException {
        RunScript.execute(getConnection(), load("drop_tables.sql"));
    }

    private Reader load(String fileName) throws IOException {
        return new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(fileName));
    }

    @Override
    protected void setStorableTests() {
        storableTests = new ArrayList<StorableTest>() {{
            add(new DataSourceTest());
            add(new DeviceJdbcTest());
            add(new ParsersTest());
            add(new DataFeedsJdbcTest());
        }};
    }
}
