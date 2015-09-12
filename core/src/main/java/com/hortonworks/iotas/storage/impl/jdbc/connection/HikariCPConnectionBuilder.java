/**
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
package com.hortonworks.iotas.storage.impl.jdbc.connection;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class HikariCPConnectionBuilder implements ConnectionBuilder {

    private Map<String, Object> configMap;
    private HikariConfig config;
    private transient HikariDataSource dataSource;

    public HikariCPConnectionBuilder(Map<String, Object> hikariCPConfig) {
        this.configMap = hikariCPConfig;
        prepare();
    }

    public HikariCPConnectionBuilder(HikariConfig hikariConfig) {
        this.config = hikariConfig;
        prepare();
    }

    @Override
    public synchronized void prepare() {
        if(dataSource == null) {
            if (configMap != null) {
                Properties properties = new Properties();
                properties.putAll(configMap);
                config = new HikariConfig(properties);
            }
            this.dataSource = new HikariDataSource(config);
            this.dataSource.setAutoCommit(false);
        }
    }

    @Override
    public Connection getConnection() {
        try {
            return this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanup() {
        if(dataSource != null) {
            dataSource.shutdown();
        }
    }
}
