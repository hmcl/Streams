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
import com.hortonworks.iotas.catalog.Device;
import com.hortonworks.iotas.storage.AbstractStoreManagerTest;
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorageManager;
import com.hortonworks.iotas.storage.impl.jdbc.connection.Config;
import com.hortonworks.iotas.storage.impl.jdbc.connection.ConnectionBuilder;
import org.h2.tools.RunScript;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Category(IntegrationTest.class)
public class JdbcStorageManagerIntegrationTest extends JdbcIntegrationTest {
    private static StorageManager jdbcStorageManager;
    private static List<Storable> devices;
    private static AbstractStoreManagerTest storageManagerTest;

    // ===== Test Setup ====

    @BeforeClass
    public static void setUpClass() throws Exception {
        JdbcIntegrationTest.setUpClass();
        setJdbcStorageManager(connectionBuilder);
        setDevices();
        setStorageManagerTest();
    }

    @Before
    public void setUp() throws Exception {
        createTablesH2();
//        createTables();
    }

    @After
    public void tearDown() throws Exception {
        dropTables();
    }

    private static void setStorageManagerTest() {
        storageManagerTest = new AbstractStoreManagerTest() {
            @Override
            public StorageManager getStorageManager() {
                return jdbcStorageManager;
            }

            @Override
            protected void setStorableTests() {
                return; //TODO
            }
        };
    }

    @Override
    public StorageManager getStorageManager() {
        return jdbcStorageManager;
    }

    //Device has foreign key in DataSource table, which has to be initialized before inserting date in Device table
    class DeviceJdbcTest extends DeviceTest {

        @Override
        public void init() {
            DataSourceTest dataSourceTest = new DataSourceTest();
            List<Storable> dataSources = dataSourceTest.getStorableList();
            for (Storable dataSource : dataSources) {
                jdbcStorageManager.addOrUpdate(dataSource);
            }
        }

        @Override
        public void close() {
            try {
                getConnection().rollback();
            } catch (SQLException e) {
                throw new RuntimeException("Exception during rollback", e);
            }
        }
    }

    @Override
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
            if (connection == null) {
                setConnection(super.config.isAutoCommit());
            }
            return connection;
        }

        private void setConnection(boolean autoCommit) throws SQLException {
            Connection connection = super.connectionBuilder.getConnection();
            connection.setAutoCommit(autoCommit);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            this.connection = connection;
        }

        @Override
        protected void closeConnection(Connection connection) {
            // Do not close the connection
        }
    }


    //TODO clean this up
    private void createTablesH2() throws SQLException, FileNotFoundException {
        RunScript.execute(getConnection(), new FileReader("/Users/hlouro/Hortonworks/Dev/GitHub/hortonworks/iotas/core/src/main/java/com/hortonworks/iotas/storage/impl/jdbc/mysql/script/db-schema.sql"));
    }

    private void createTables() throws SQLException {
        final String sql = "CREATE TABLE IF NOT EXISTS devices (\n" +
                "    deviceId VARCHAR(64) NOT NULL,\n" +
                "    version BIGINT NOT NULL,\n" +
                "    dataSourceId BIGINT NOT NULL,\n" +
                "    PRIMARY KEY (deviceId, version)\n" +
                ");";

        executeSql(sql);
    }

    private void dropTables() throws SQLException {
        final String sql = "DROP TABLE IF EXISTS datafeeds";
        final String sql1 = "DROP TABLE IF EXISTS parser_info";
        final String sql2 = "DROP TABLE IF EXISTS devices";
        final String sql3 = "DROP TABLE IF EXISTS datasources";
        executeSql(sql);
        executeSql(sql1);
        executeSql(sql2);
        executeSql(sql3);
    }

    private static void setJdbcStorageManager(ConnectionBuilder connectionBuilder) {
//        jdbcStorageManager = new JdbcStorageManager(connectionBuilder, new Config(-1, false));
        jdbcStorageManager = new JdbcStorageManagerForTest(connectionBuilder, new Config(-1, false));
    }

    @Override
    protected void setStorableTests() {
        storableTests = new ArrayList<StorableTest>() {{
            add(new DataSourceTest());
            add(new DeviceJdbcTest());
//            add(new ParsersTest());
//            add(new DataFeedsTest());
        }};
    }

    private static void setDevices() {
        devices = new ArrayList<>();
        final Map<String, Object> state = new HashMap<>();
        state.put(Device.DEVICE_ID, "device_id_test");
        state.put(Device.VERSION, 123L);
        state.put(Device.DATA_SOURCE_ID, 456L);
        devices.add(newDevice(state));

        // device with same StorableKey but that does not verify .equals()
        state.put(Device.DATA_SOURCE_ID, 789L);
        devices.add(newDevice(state));
    }

    private static Device newDevice(Map state) {
        Device device = new Device();
        device.fromMap(state);
        return device;
    }

    // ===== Test Methods =====
    // Test methods use the widely accepted naming convention  [UnitOfWork_StateUnderTest_ExpectedBehavior]

    //TODO Need to test queryParamsOfAllTypes

    @Test
    public void testCrudForAllEntities() {
        super.testCrudForAllEntities();
//        storageManagerTest.testCrudForAllEntities();
    }

    /*@Test
    public void testCrud() throws Exception {
        StorableKey key = devices.getStorableKey();;
        Storable retrieved;
        try {
            testGet_nonExistentKey_null();
            jdbcStorageManager.add(devices);
            retrieved = jdbcStorageManager.get(key);
            assertEquals("Instance put and retrieved from database are different", devices, retrieved);
        } finally {
            jdbcStorageManager.remove(key);
            retrieved = jdbcStorageManager.get(key);
            assertNull(retrieved);
        }
    }*/

    /*@Test
    public void testGet_nonExistentKey_null() {
        final Storable device = devices.get(0);
        final Storable retrieved = jdbcStorageManager.get(device.getStorableKey());
        assertNull(retrieved);
    }

    @Test
    public void testAdd_nonExistentStorable_void() {
        Storable device = devices.get(0);
        jdbcStorageManager.add(device);
        testGet_existingStorable_existingStorable(device);
    }

    // UnequalExistingStorable => Storable that has the same StorableKey but does NOT verify .equals()
    @Test(expected = AlreadyExistsException.class)
    public void testAdd_unequalExistingStorable_AlreadyExistsException() {
        Assert.assertNotEquals(devices.get(0), devices.get(1));
        jdbcStorageManager.add(devices.get(0));
        jdbcStorageManager.add(devices.get(1));     // should throw exception
    }

    // EqualExistingStorable => Storable that has the same StorableKey and verifies .equals()
    @Test
    public void testAdd_equalExistingStorable_void() {
        final Storable device = devices.get(0);
        jdbcStorageManager.add(device);
        jdbcStorageManager.add(device);
        testGet_existingStorable_existingStorable(device);
    }

    @Test
    public void testAddOrUpdate_nonExistentStorable_void() {
        Storable device = devices.get(0);
        jdbcStorageManager.addOrUpdate(device);
        testGet_existingStorable_existingStorable(device);
    }

    // UnequalExistingStorable => Storable that has the same StorableKey but does NOT verify .equals()
    @Test
    public void testAddOrUpdate_unequalExistingStorable_void() {
        final Storable device0 = devices.get(0);
        final Storable device1 = devices.get(1);
        Assert.assertNotEquals(device0, device1);
        jdbcStorageManager.addOrUpdate(device0);
        jdbcStorageManager.addOrUpdate(device1);
        testGet_existingStorable_existingStorable(device1);
    }

    // EqualExistingStorable => Storable that has the same StorableKey and verifies .equals()
    @Test
    public void testAddOrUpdate_equalExistingStorable_void() {
        final Storable device = devices.get(0);
        jdbcStorageManager.add(device);
        jdbcStorageManager.add(device);
        testGet_existingStorable_existingStorable(device);
    }

    @Test
    public void testRemove_existingStorable_existingStorable() {
        final Storable device = devices.get(0);
        jdbcStorageManager.add(device);
        testGet_existingStorable_existingStorable(device);
        final StorableKey key = device.getStorableKey();
        Storable removed = jdbcStorageManager.remove(key);
        assertNotNull(removed);
        assertEquals(device, removed);
    }

    @Test
    public void testRemove_NonExistentStorable_null() {

    }



    *//*@Test
    public void testAddOrUpdate_newStorable_newStorable() {
        testAdd_nonExistentStorable_void();
        Storable update = newDevice("new_device_id_test", 7L, 8L);
        jdbcStorageManager.addOrUpdate(update);
        dotestAdd_Storable_Get_StorableKey_Storable(update);
    }*//*


    public void testAdd_newStorable_AlreadyExistsException() {


    }

    // TODO TEST INSERT DUPLICATE KEY
    // TODO Test null Storable and StorableKey values
    @Test
    public void testAddDuplicateStorable() {
        //TODO
    }

    // ======= private helper methods ========
    private void testGet_existingStorable_existingStorable(Storable existing) {
        Storable retrieved = jdbcStorageManager.get(existing.getStorableKey());
        assertEquals("Instance put and retrieved from database are different", existing, retrieved);
    }*/


}
