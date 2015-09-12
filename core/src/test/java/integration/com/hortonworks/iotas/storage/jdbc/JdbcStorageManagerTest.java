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

package integration.com.hortonworks.iotas.storage.jdbc;

import com.hortonworks.iotas.catalog.Device;
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorableKey;
import com.hortonworks.iotas.storage.StorageManager;
import com.hortonworks.iotas.storage.impl.jdbc.JdbcStorageManager;
import com.hortonworks.iotas.storage.impl.jdbc.connection.ConnectionBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.connection.HikariCPConnectionBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlBuilder;
import org.h2.tools.DeleteDbFiles;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class JdbcStorageManagerTest {
    // TODO
    //create DB
    //put stuff
    //take stuff
    private StorageManager jdbcStorageManager;
    private Storable device;
    private ConnectionBuilder connectionBuilder;

    @Before
    public void setUp() throws Exception {
        cleanup();
//        setConnectionBuilder2();
        setConnectionBuilder();
        setJdbcStorageManager(connectionBuilder);
        setDevice();
        createTables();
    }

    @After
    public void tearDown() throws Exception {
        dropTables();
    }

    private void cleanup()  {
        String dbDir = "/Users/hlouro/Hugo/tmp/db/h2mysql_test/";
        String db = dbDir + "test";
        DeleteDbFiles.execute(dbDir, "test", false);
    }

    private void dropTables() {

    }

    private class JdbcStorageManagerForTest extends JdbcStorageManager {
        private Connection con;
        public JdbcStorageManagerForTest(ConnectionBuilder connectionBuilder) {
            super(connectionBuilder);
//            setConection();
//            con = connectionBuilder.getConnection();
        }

        private void setConection() {
            try {
                Class.forName("org.h2.Driver");
                con = DriverManager.getConnection("jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql_test/test;MODE=MySQL");
            } catch (ClassNotFoundException | SQLException e) {
                throw new RuntimeException(e);
            }
        }


        @Override
        protected Connection getConnection() throws SQLException {
            return super.getConnection();
//            return con;
        }

        @Override
        protected void closeConnection(Connection connection) {
            // do nothing
        }
    }

    private void createTables() throws SQLException {
        final String sql = "CREATE TABLE IF NOT EXISTS devices (\n" +
                "    deviceId VARCHAR(64) NOT NULL,\n" +
                "    version BIGINT NOT NULL,\n" +
                "    dataSourceId BIGINT NOT NULL,\n" +
                "    PRIMARY KEY (deviceId, version)\n" +
                ");";

//        final String sql = "XXX";

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

    private void setJdbcStorageManager(ConnectionBuilder connectionBuilder) {
//        jdbcStorageManager = new JdbcStorageManagerForTest(connectionBuilder);
        jdbcStorageManager = new JdbcStorageManager(connectionBuilder);
    }


    private void setConnectionBuilder2() {
        connectionBuilder = new ConnectionBuilder() {
            Connection con;
            @Override
            public void prepare() { }

            @Override
            public Connection getConnection() {
                try {
                    if (con == null) {
                        Class.forName("org.h2.Driver");
                        con = DriverManager.getConnection("jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql_test/test;MODE=MySQL;DATABASE_TO_UPPER=false");
                    }
                } catch (ClassNotFoundException | SQLException e) {
                    throw new RuntimeException(e);
                }
                return con;
            }

            @Override
            public void cleanup() { }
        };
    }
    private void setConnectionBuilder() {
//        Map<String, Object> config = getMySqlHikariConfig();
        Map<String, Object> config = getH2HikariConfig();
//        HikariConfig config = getH2HikariConfig();
        connectionBuilder = new HikariCPConnectionBuilder(config);
        System.out.println("stop");
    }

    private Map<String, Object> getMySqlHikariConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        config.put("dataSource.url", "jdbc:mysql://localhost/test");
        config.put("dataSource.user", "root");
        return config;
    }

    private Map<String, Object> getH2HikariConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("dataSourceClassName", "org.h2.jdbcx.JdbcDataSource");
//        config.put("dataSource.URL", "jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql_test/test");
        config.put("dataSource.URL", "jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql_test/test;MODE=MySQL;DATABASE_TO_UPPER=false"); // ;FILE_LOCK=SOCKET
//        config.put("dataSource.url", "jdbc:h2:mem:test;MODE=MySQL;FILE_LOCK=SOCKET");
//        config.put("dataSource.url", "jdbc:h2:mem:test");
//        config.put("dataSource.url", "jdbc:h2:file:/Users/hlouro/Hugo/tmp/db/h2mysql_test/test;MODE=MySQL;MV_STORE=FALSE;MVCC=FALSE");
//        config.put("dataSource.user", "sa");
//        config.put("dataSource.URL", "jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql_test/test;MODE=MySQL;INIT=RUNSCRIPT FROM '/Users/hlouro/Hugo/tmp/db/h2mysql_test/test.sql'");
//        config.put("dataSource.url", "jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql/test;MODE=MySQL");
        return config;
    }

    /*private HikariConfig getH2HikariConfig() {
        HikariConfig config = new HikariConfig();

        config.setDataSourceClassName("org.h2.jdbcx.JdbcDataSource");

//        config.setConnectionTestQuery("VALUES 1");
        config.setConnectionTestQuery(sql);

//        config.addDataSourceProperty("URL", "jdbc:h2:~/test");
//        config.addDataSourceProperty("URL", "jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql_test_guy/test");
//        config.addDataSourceProperty("URL", "jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql_test_guy/test;MODE=MySQL");

        config.addDataSourceProperty("URL", "jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql_test/test;MODE=MySQL");

//        config.addDataSourceProperty("user", "sa");
//
//        config.addDataSourceProperty("password", "sa");

        return config;
    }*/

    @Test
    public void testCrud() throws Exception {
        StorableKey key = null;
        Storable retrieved;
        try {
            key = device.getStorableKey();
            retrieved = jdbcStorageManager.get(key);
            Assert.assertNull(retrieved);
            jdbcStorageManager.add(device);
            retrieved = jdbcStorageManager.get(key);
            Assert.assertEquals("Instance put and retrieved from database are different", device, retrieved);
            Assert.assertEquals("Instance put and retrieved from database are different", device, retrieved);
        } finally {
            jdbcStorageManager.remove(key);
            retrieved = jdbcStorageManager.get(key);
            Assert.assertNull(retrieved);
        }
    }

    @Test
    public void testGet() throws Exception {
        StorableKey key = device.getStorableKey();
        System.out.println("StorableKey = " + key);
        Storable retrieved = jdbcStorageManager.get(key);
        Assert.assertEquals("Instance put and retrieved from database are different", device, retrieved);
    }

    // TODO TEST INSERT DUPLICATE KEY
    @Test
    public void testAddDuplicateStorable() {
        //TODO
    }

    private void setDevice() {
        this.device = new Device();
        Map<String, Object> state = new HashMap<>();
        state.put(Device.DEVICE_ID, "device_id_test");
        state.put(Device.VERSION, 123L);
        state.put(Device.DATA_SOURCE_ID, 456L);
        device.fromMap(state);
    }


//    public static void main(String[] args) throws Exception {
//        JdbcStorageManagerTest jdbcStorageManagerTest = new JdbcStorageManagerTest();
//        jdbcStorageManagerTest.setUp();
//        jdbcStorageManagerTest.testCrud();
//    }
}
