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

package com.hortonworks.iotas.storage.impl.jdbc.mysql.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Executor {
    public static void executeSql(Connection connection, final String sql) throws SQLException {
        new MySqlBuilder() {
            @Override
            public String getParameterizedSql() {
                return sql;
            }

            @Override
            public PreparedStatement getPreparedStatement(Connection connection, int queryTimeoutSecs) throws SQLException {
                return this.prepareStatement(connection, queryTimeoutSecs);
            }
        }.getPreparedStatement(connection, -1).execute();
    }
}
