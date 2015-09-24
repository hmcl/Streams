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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.hortonworks.iotas.common.Schema;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

public abstract class MySqlBuilder implements SqlBuilder {
    protected static final Logger log = LoggerFactory.getLogger(MySqlBuilder.class);
    protected List<Schema.Field> columns;
    protected String tableName;
    protected String paramSql;
    protected PreparedStatement preparedStatement;

    @Override
    public List<Schema.Field> getColumns() {
        return columns;
    }

    @Override
    public abstract String getParametrizedSql();

    public abstract PreparedStatement getParametrizedPreparedStatement(Connection connection, int queryTimeoutSecs) throws SQLException;

    /**
     * @return The prepared statement with parameters
     */
    protected PreparedStatement prepareStatement(Connection connection, int queryTimeoutSecs) throws SQLException {

        String parameterizedSql = getParametrizedSql();
        log.debug("Creating prepared statement with parameterized sql [{}]", parameterizedSql);
        PreparedStatement preparedStatement = connection.prepareStatement(parameterizedSql);

        if (queryTimeoutSecs > 0) {
            preparedStatement.setQueryTimeout(queryTimeoutSecs);
        }
        return preparedStatement;
    }

    protected String join(Collection<String> in, String separator) {
        return Joiner.on(separator).join(in);
    }

    /**
     * @param num number of times to repeat the pattern
     * @return bind variables repeated num times
     */
    protected String getBindVariables(String pattern, int num) {
        return StringUtils.chop(StringUtils.repeat(pattern, num));
    }

    /**
     * if formatter != null applies the formatter to the column names. Examples of output are:
     * <p/>
     * formatter == null ==> [colName1, colName2]
     * <p/>
     * formatter == "%s = ?" ==> [colName1 = ?, colName2 = ?]
     */
    protected Collection<String> getColumnNames(Collection<Schema.Field> columns, final String formatter) {
        return Collections2.transform(columns, new Function<Schema.Field, String>() {
            @Override
            public String apply(Schema.Field field) {
                return formatter == null ? field.getName() : String.format(formatter, field.getName());
            }
        });
    }

    protected void setPreparedStatementParams(PreparedStatement preparedStatement,
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

    @Override
    public String toString() {
        return "MySqlBuilder{" +
                "columns=" + columns +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
