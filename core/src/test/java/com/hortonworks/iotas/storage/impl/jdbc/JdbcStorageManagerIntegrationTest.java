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
import com.hortonworks.iotas.storage.AlreadyExistsException;
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorageManager;
import com.hortonworks.iotas.storage.impl.jdbc.connection.ConnectionBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Category(IntegrationTest.class)
public class JdbcStorageManagerIntegrationTest extends JdbcIntegrationTest {
    // TODO
    //create DB
    //put stuff
    //take stuff
    private static StorageManager jdbcStorageManager;
    private static List<Storable> devices;

    // ===== Test Setup ====

    @BeforeClass
    public static void setUpClass() throws Exception {
        JdbcIntegrationTest.setUpClass();
        setJdbcStorageManager(connectionBuilder);
        setDevices();
    }

    @Before
    public void setUp() throws Exception {
        createTables();
    }

    @After
    public void tearDown() throws Exception {
        dropTables();
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
        final String sql = "DROP TABLE IF EXISTS devices";
        executeSql(sql);
    }

    private static void setJdbcStorageManager(ConnectionBuilder connectionBuilder) {
        jdbcStorageManager = new JdbcStorageManager(connectionBuilder);
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

    @Test
    public void testGet_nonExistentKey_null() {
        final Storable device = devices.get(0);
        final Storable retrieved = jdbcStorageManager.get(device.getStorableKey());
        assertNull(retrieved);
    }

    @Test
    public void testAdd_nonExistentStorable_void() {
        Storable device = devices.get(0);
        jdbcStorageManager.add(device);
        testGet_ExistingStorable_ExistingStorable(device);
    }

    // UnequalExistingStorable => Storable that has the same StorableKey but does NOT verify .equals()
    @Test(expected = AlreadyExistsException.class)
    public void testAdd_UnequalExistingStorable_AlreadyExistsException() {
        Assert.assertNotEquals(devices.get(0), devices.get(1));
        jdbcStorageManager.add(devices.get(0));
        jdbcStorageManager.add(devices.get(1));     // should throw exception
    }

    // EqualExistingStorable => Storable that has the same StorableKey and verifies .equals()
    @Test
    public void testAdd_EqualExistingStorable_void() {
        final Storable device = devices.get(0);
        jdbcStorageManager.add(device);
        jdbcStorageManager.add(device);
        testGet_ExistingStorable_ExistingStorable(device);
    }

    @Test
    public void testAddOrUpdate_nonExistentStorable_void() {
        Storable device = devices.get(0);
        jdbcStorageManager.addOrUpdate(device);
        testGet_ExistingStorable_ExistingStorable(device);
    }

    // UnequalExistingStorable => Storable that has the same StorableKey but does NOT verify .equals()
    @Test
    public void testAddOrUpdate_UnequalExistingStorable_void() {
        final Storable device0 = devices.get(0);
        final Storable device1 = devices.get(1);
        Assert.assertNotEquals(device0, device1);
        jdbcStorageManager.addOrUpdate(device0);
        jdbcStorageManager.addOrUpdate(device1);
        testGet_ExistingStorable_ExistingStorable(device1);
    }

    // EqualExistingStorable => Storable that has the same StorableKey and verifies .equals()
    @Test
    public void testAddOrUpdate_EqualExistingStorable_void() {
        final Storable device = devices.get(0);
        jdbcStorageManager.add(device);
        jdbcStorageManager.add(device);
        testGet_ExistingStorable_ExistingStorable(device);
    }



    /*@Test
    public void testAddOrUpdate_newStorable_newStorable() {
        testAdd_nonExistentStorable_void();
        Storable update = newDevice("new_device_id_test", 7L, 8L);
        jdbcStorageManager.addOrUpdate(update);
        dotestAdd_Storable_Get_StorableKey_Storable(update);
    }*/


    public void testAdd_newStorable_AlreadyExistsException() {


    }

    // TODO TEST INSERT DUPLICATE KEY
    @Test
    public void testAddDuplicateStorable() {
        //TODO
    }

    // ======= private helper methods ========
    private void testGet_ExistingStorable_ExistingStorable(Storable existing) {
        Storable retrieved = jdbcStorageManager.get(existing.getStorableKey());
        assertEquals("Instance put and retrieved from database are different", existing, retrieved);
    }


}
