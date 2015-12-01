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

package com.hortonworks.iotas.layout.runtime.rule.sql;

import org.apache.storm.sql.runtime.DataSource;
import org.apache.storm.sql.runtime.DataSourcesProvider;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;

import java.net.URI;
import java.util.List;

public class RulesDataSourcesProvider implements DataSourcesProvider {
    public static DataSourcesProvider delegate;

    @Override
    public String scheme() {
        return delegate.scheme();
    }

    @Override
    public DataSource construct(URI uri, String s, String s1, List<FieldInfo> list) {
        return delegate.construct(uri, s, s1, list);
    }

    @Override
    public ISqlTridentDataSource constructTrident(URI uri, String s, String s1, List<FieldInfo> list) {
        return delegate.constructTrident(uri, s, s1, list);
    }
}
