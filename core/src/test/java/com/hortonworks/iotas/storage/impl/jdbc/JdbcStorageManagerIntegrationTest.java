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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

@Category(IntegrationTest.class)
public class JdbcStorageManagerIntegrationTest extends AbstractStoreManagerTest {
    private static StorageManager jdbcStorageManager;

    // ===== Test Setup ====

    @BeforeClass
    public static void setUpClass() throws Exception {
        setJdbcStorageManager(new HikariCPConnectionBuilder(HikariBasicConfig.getMySqlHikariTestConfig()));
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
//        jdbcStorageManager = new JdbcStorageManager(connectionBuilder, new Config(-1, false));
        jdbcStorageManager = new JdbcStorageManagerForTest(connectionBuilder, new Config(-1, false));
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


    //TODO clean this up
    private void createTables() throws SQLException, FileNotFoundException {
        RunScript.execute(getConnection(), new FileReader("/Users/hlouro/Hortonworks/Dev/GitHub/hortonworks/iotas/core/src/main/java/com/hortonworks/iotas/storage/impl/jdbc/mysql/schema/create_tables.sql"));
    }

    private void dropTables() throws SQLException, FileNotFoundException {
        RunScript.execute(getConnection(), new FileReader("/Users/hlouro/Hortonworks/Dev/GitHub/hortonworks/iotas/core/src/main/java/com/hortonworks/iotas/storage/impl/jdbc/mysql/schema/drop_tables.sql"));

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
