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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class JdbcStorageManagerTestClean {
    private StorageManager jdbcStorageManager;
    private Storable device;
    private ConnectionBuilder connectionBuilder;

    @Before
    public void setUp() throws Exception {
        setConnectionBuilder2();
        setJdbcStorageManager(connectionBuilder);
        setDevice();
    }

    private void setConnectionBuilder2() {
        System.out.println("Called setConnectionBuilder2");
        connectionBuilder = new ConnectionBuilder() {
            private Connection con;
            @Override
            public void prepare() { /*Do Nothing*/ }

            @Override
            public Connection getConnection() {
                try {
                    if (con == null) {
                        Class.forName("org.h2.Driver");
                        con = DriverManager.getConnection("jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql_test/test;MODE=MySQL");
                    }
                } catch (ClassNotFoundException | SQLException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("Returning connection: " + con);
                return con;
            }

            @Override
            public void cleanup() { /*Do Nothing*/ }
        };
    }

    private void setJdbcStorageManager(ConnectionBuilder connectionBuilder) {
        jdbcStorageManager = new JdbcStorageManagerForTest(connectionBuilder);
//        jdbcStorageManager = new JdbcStorageManager(connectionBuilder);
    }

    private class JdbcStorageManagerForTest extends JdbcStorageManager {
        public JdbcStorageManagerForTest(ConnectionBuilder connectionBuilder) {
            super(connectionBuilder);
        }

        @Override
        protected Connection getConnection() throws SQLException {
            return super.getConnection();
        }

        @Override
        protected void closeConnection(Connection connection) {
            // do nothing
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

    @Test
    public void testGet() throws Exception {
        StorableKey key = device.getStorableKey();
        System.out.println("StorableKey = " + key);
        Storable retrieved = jdbcStorageManager.get(key);
        Assert.assertEquals("Instance put and retrieved from database are different", device, retrieved);
    }


}
