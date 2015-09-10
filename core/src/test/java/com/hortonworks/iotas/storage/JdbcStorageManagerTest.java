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

package com.hortonworks.iotas.storage;

import com.hortonworks.iotas.catalog.Device;
import com.hortonworks.iotas.storage.impl.jdbc.JdbcStorageManager;
import com.hortonworks.iotas.storage.impl.jdbc.connection.ConnectionBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.connection.HikariCPConnectionBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class JdbcStorageManagerTest {
    // TODO
    //create DB
    //put stuff
    //take stuff
    private StorageManager jdbcStorageManager;
    private Storable device;

    @Before
    public void setUp() throws Exception {
        setJdbcStorageManager();
        setDevice();
        createTables();
    }

    @After
    public void tearDown() throws Exception {
        dropTables();
    }

    private void dropTables() {

    }

    private void createTables() {

    }

    private void setJdbcStorageManager() {
        Map<String, Object> config = getHikariConfig();
        ConnectionBuilder connectionBuilder = new HikariCPConnectionBuilder(config);
        this.jdbcStorageManager = new JdbcStorageManager(connectionBuilder);
    }

    private Map<String, Object> getHikariConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        config.put("dataSource.url", "jdbc:mysql://localhost/test");
        config.put("dataSource.user", "root");
        return config;
    }

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

    private void setDevice() {
        this.device = new Device();
        Map<String, Object> state = new HashMap<>();
        state.put(Device.DEVICE_ID, "device_id_test");
        state.put(Device.VERSION, 123L);
        state.put(Device.DATA_SOURCE_ID, 456L);
        device.fromMap(state);
    }

    // TODO TEST INSERT DUPLICATE KEY
}
