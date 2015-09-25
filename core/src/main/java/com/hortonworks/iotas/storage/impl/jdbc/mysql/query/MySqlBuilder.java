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
import com.hortonworks.iotas.storage.PrimaryKey;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

public abstract class MySqlBuilder implements SqlBuilder {
    protected static final Logger log = LoggerFactory.getLogger(MySqlBuilder.class);
    protected List<Schema.Field> columns;
    protected String tableName;
    protected PrimaryKey primaryKey;
    protected String sql;

    protected abstract void setParameterizedSql();

    @Override
    public String getParametrizedSql() {
        return sql;
    }

    @Override
    public List<Schema.Field> getColumns() {
        return columns;
    }

    @Override
    public String getNamespace() {
        return tableName;
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    // ==== helper methods used in the query construction process ======

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

    @Override
    public String toString() {
        return "MySqlBuilder{" +
                "columns=" + columns +
                ", tableName='" + tableName + '\'' +
                ", primaryKey=" + primaryKey +
                ", sql='" + sql + '\'' +
                '}';
    }
}
