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
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorableKey;
import com.hortonworks.iotas.storage.StorageManager;
import com.hortonworks.iotas.storage.impl.jdbc.connection.ConnectionBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Category(IntegrationTest.class)
public class JdbcStorageManagerIntegrationTest extends JdbcIntegrationTest {
    // TODO
    //create DB
    //put stuff
    //take stuff
    private StorageManager jdbcStorageManager;
    private Storable device;

    // ===== Test Setup ====

    @Before
    public void setUp() throws Exception {
        setJdbcStorageManager(connectionBuilder);
        setDevice();
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

    private void setJdbcStorageManager(ConnectionBuilder connectionBuilder) {
        jdbcStorageManager = new JdbcStorageManager(connectionBuilder);
    }


    private void setDevice() {
        this.device = new Device();
        Map<String, Object> state = new HashMap<>();
        state.put(Device.DEVICE_ID, "device_id_test");
        state.put(Device.VERSION, 123L);
        state.put(Device.DATA_SOURCE_ID, 456L);
        device.fromMap(state);
    }

    // ===== Test Methods ====

    @Test
    public void testCrud() throws Exception {
        System.out.println("HUGO_TEST");
        StorableKey key = null;
        Storable retrieved;
        try {
            key = device.getStorableKey();
            retrieved = jdbcStorageManager.get(key);
            assertNull(retrieved);
            jdbcStorageManager.add(device);
            retrieved = jdbcStorageManager.get(key);
            assertEquals("Instance put and retrieved from database are different", device, retrieved);
        } finally {
            jdbcStorageManager.remove(key);
            retrieved = jdbcStorageManager.get(key);
            assertNull(retrieved);
        }
    }

    // TODO TEST INSERT DUPLICATE KEY
    @Test
    public void testAddDuplicateStorable() {
        //TODO
    }


}
