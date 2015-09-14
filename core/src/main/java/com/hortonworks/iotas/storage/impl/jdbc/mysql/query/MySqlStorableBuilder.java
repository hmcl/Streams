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
package com.hortonworks.iotas.storage.impl.jdbc.mysql.query;

import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorableKey;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

public abstract class MySqlStorableBuilder extends MySqlBuilder {
    protected Storable storable;

    public MySqlStorableBuilder(Storable storable) {
        this.storable = storable;
        final StorableKey storableKey = storable.getStorableKey();
        tableName = storableKey.getNameSpace();
        columns = storable.getSchema().getFields();
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, int queryTimeoutSecs) throws SQLException {
        return doGetPreparedStatement(connection, queryTimeoutSecs, 1);
    }

    protected PreparedStatement doGetPreparedStatement(Connection connection, int queryTimeoutSecs, int nTimes) throws SQLException {
        final PreparedStatement preparedStatement = prepareStatement(connection, queryTimeoutSecs);

        final int len = columns.size();
        final Map columnsToValues = storable.toMap();

        for (int j = 0; j < len*nTimes; j++) {
            Schema.Field column = columns.get(j % len);
            Schema.Type javaType = column.getType();
            String columnName = column.getName();
            setPreparedStatementParams(preparedStatement, javaType, j + 1, columnsToValues.get(columnName));
        }

        return preparedStatement;
    }
}