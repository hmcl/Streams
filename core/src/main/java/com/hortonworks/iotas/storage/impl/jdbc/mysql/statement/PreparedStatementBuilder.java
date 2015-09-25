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

package com.hortonworks.iotas.storage.impl.jdbc.mysql.statement;

import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.storage.exception.MalformedQueryException;
import com.hortonworks.iotas.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlStorableBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlStorableKeyBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.SqlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PreparedStatementBuilder {
    private static final Logger log = LoggerFactory.getLogger(PreparedStatementBuilder.class);
    private final Connection connection;
    private PreparedStatement preparedStatement;
    private final SqlBuilder sqlBuilder;
    private final ExecutionConfig config;
    private int numParams;                          // Number of prepared statement parameters

    public PreparedStatementBuilder(Connection connection, ExecutionConfig config,
                                    SqlBuilder sqlBuilder) throws SQLException {
        this.connection = connection;
        this.config = config;
        this.sqlBuilder = sqlBuilder;
        setPreparedStatement();
        setNumParams();
    }

    /** Creates the prepared statement with the parameters in place to be replaced */
    private void setPreparedStatement() throws SQLException {

        final String parameterizedSql = sqlBuilder.getParametrizedSql();
        log.debug("Creating prepared statement for parameterized sql [{}]", parameterizedSql);

        final PreparedStatement preparedStatement = connection.prepareStatement(parameterizedSql);
        final int queryTimeoutSecs = config.getQueryTimeoutSecs();
        if (queryTimeoutSecs > 0) {
            preparedStatement.setQueryTimeout(queryTimeoutSecs);
        }
        this.preparedStatement = preparedStatement;
    }

    private void setNumParams() {
        Pattern p = Pattern.compile("[?]");
        Matcher m = p.matcher(sqlBuilder.getParametrizedSql());
        int groupCount = 0;
        while (m.find()) {
            groupCount++;
        }
        log.debug("{} ? query parameters found for {} ", groupCount, sqlBuilder.getParametrizedSql());

        if (!isNumColumnsMultipleOfNumParameters(sqlBuilder, groupCount)) {
            throw new MalformedQueryException("Number of columns must be a multiple of the number of query parameters");
        }
        numParams = groupCount;
    }

    private boolean isNumColumnsMultipleOfNumParameters(SqlBuilder sqlBuilder, int groupCount) {
        final List<Schema.Field> columns = sqlBuilder.getColumns();
        boolean isMultiple = false;

        if (columns == null || columns.size() == 0) {
            isMultiple = groupCount == 0;
        } else {
            isMultiple = ((groupCount % sqlBuilder.getColumns().size()) == 0);
        }
        return isMultiple;
    }

    public PreparedStatement getPreparedStatement(SqlBuilder sqlBuilder) throws SQLException {
        // If more types become available consider subclassing instead of going with this approach, which was chosen here for simplicity
        if (sqlBuilder instanceof MySqlStorableKeyBuilder) {
            return getStorableKeyPreparedStatement(sqlBuilder);
        } else if (sqlBuilder instanceof MySqlStorableBuilder) {
            return getStorablePreparedStatement(sqlBuilder);
        } else {
            return preparedStatement;
        }
    }

    private PreparedStatement getStorableKeyPreparedStatement(SqlBuilder sqlBuilder) throws SQLException {
        final List<Schema.Field> columns = sqlBuilder.getColumns();

        if (columns != null) {
            final int len = columns.size();
            Map<Schema.Field, Object> columnsToValues = sqlBuilder.getPrimaryKey().getFieldsToVal();

            int nTimes = numParams/len;   // Number of times each column must be replaced on a query parameter
            for (int j = 0; j < len * nTimes; j++) {
                Schema.Field column = columns.get(j % len);
                Schema.Type javaType = column.getType();
                setPreparedStatementParams(preparedStatement, javaType, j + 1, columnsToValues.get(column));
            }
        }
        return preparedStatement;
    }

    private PreparedStatement getStorablePreparedStatement(SqlBuilder sqlBuilder) throws SQLException {
        final List<Schema.Field> columns = sqlBuilder.getColumns();

        if (columns != null) {
            final int len = columns.size();
            final Map columnsToValues = ((MySqlStorableBuilder)sqlBuilder).getStorable().toMap();
            final int nTimes = numParams/len;   // Number of times each column must be replaced on a query parameter

            for (int j = 0; j < len*nTimes; j++) {
                Schema.Field column = columns.get(j % len);
                Schema.Type javaType = column.getType();
                String columnName = column.getName();
                setPreparedStatementParams(preparedStatement, javaType, j + 1, columnsToValues.get(columnName));
            }
        }
        return preparedStatement;
    }

    public Connection getConnection() {
        return connection;
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return preparedStatement.getMetaData();
    }

    @Override
    public String toString() {
        return "PreparedStatementBuilder{" +
                "sqlBuilder=" + sqlBuilder +
                ", numParams=" + numParams +
                ", connection=" + connection +
                ", preparedStatement=" + preparedStatement +
                ", config=" + config +
                '}';
    }

    private void setPreparedStatementParams(PreparedStatement preparedStatement,
                                              Schema.Type type, int index, Object val) throws SQLException {
        switch (type) {
            case BOOLEAN:
                preparedStatement.setBoolean(index, (Boolean) val);
                break;
            case BYTE:
                preparedStatement.setByte(index, (Byte) val);
                break;
            case SHORT:
                preparedStatement.setShort(index, (Short) val);
                break;
            case INTEGER:
                preparedStatement.setInt(index, (Integer) val);
                break;
            case LONG:
                preparedStatement.setLong(index, (Long) val);
                break;
            case FLOAT:
                preparedStatement.setFloat(index, (Float) val);
                break;
            case DOUBLE:
                preparedStatement.setDouble(index, (Double) val);
                break;
            case STRING:
                preparedStatement.setString(index, (String) val);
                break;
            case BINARY:
                preparedStatement.setBytes(index, (byte[]) val);
                break;
            case NESTED:
            case ARRAY:
                preparedStatement.setObject(index, val);    //TODO check this
                break;
        }
    }
}
