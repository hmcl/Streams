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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
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
        setConnectionBuilder();
        setJdbcStorageManager(connectionBuilder);
        setDevice();
        createTables();
    }

    @After
    public void tearDown() throws Exception {
        dropTables();
    }

    private void dropTables() {

    }

    private final String sql = "CREATE TABLE IF NOT EXISTS devices (\n" +
            "    deviceId VARCHAR(64) NOT NULL,\n" +
            "    version BIGINT NOT NULL,\n" +
            "    dataSourceId BIGINT NOT NULL,\n" +
            "    PRIMARY KEY (deviceId, version)\n" +
            ");";


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
        this.jdbcStorageManager = new JdbcStorageManager(connectionBuilder);
    }

    private void setConnectionBuilder() {
        Map<String, Object> config = getMySqlHikariConfig();
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
//        config.put("dataSource.URL", "jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql_test/test;MODE=MySQL");
        config.put("dataSource.URL", "jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql_test/test;MODE=MySQL;MV_STORE;MVCC=FALSE");
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
        } finally {
            jdbcStorageManager.remove(key);
            retrieved = jdbcStorageManager.get(key);
            Assert.assertNull(retrieved);
        }
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


}
