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


import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.hortonworks.iotas.layout.runtime.rule.condition.expression.SqlStreamExpression;
import com.hortonworks.iotas.layout.runtime.rule.condition.script.engine.ScriptEngine;
import org.apache.storm.sql.StormSql;
import org.apache.storm.sql.runtime.ChannelContext;
import org.apache.storm.sql.runtime.ChannelHandler;
import org.apache.storm.sql.runtime.DataSource;
import org.apache.storm.sql.runtime.DataSourcesProvider;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;

//TODO
public class SqlStreamEngine implements ScriptEngine<SqlStreamEngine> {
    protected static final Logger log = LoggerFactory.getLogger(SqlStreamEngine.class);

    @Override
    public SqlStreamEngine getEngine() {
        return this;
    }

    private DataSource dataSource;                      // step 1
    private ChannelContext channelContext;              // step 2 - Data Source sets context
    private ChannelHandler channelHandler;              // step 3
    private DataSourcesProvider dataSourceProvider;     // step 4
    private boolean evaluates;

    // TODO: Revisit
    // Doing work in the constructor is not ideal but all of these inner classes make the code much simpler
    // and avoids lots of callbacks. However, this should not be an issue for testing as this is a very focused
    // class that has a very specific purpose and therefore is very unlikely to change.
    // Furthermore, the SQL streaming framework is still under development and it's API is subject to changing,
    // so for now this is a reasonable solution
    public SqlStreamEngine() {
        // This sequence of steps cannot be changed
        this.dataSource = this.new RulesDataSource();                   // Step 1 && Step 2 - RulesDataSource Sets Channel Context
        this.channelHandler = this.new RulesChannelHandler();           // Step 3
        this.dataSourceProvider = this.new RulesDataSourcesProvider();  // Step 4
    }

    public void compileQuery(List<String> statements) {
        try {
            log.debug("Compiling query statements {}", statements);
            StormSql stormSql = StormSql.construct();
            stormSql.execute(statements, channelHandler);
            log.debug("Query statements successfully compiled");
        } catch (Exception e) {
            throw new RuntimeException(String.format("Error compiling query. Statements [%s]", statements), e);
        }
    }

    public boolean eval(Values input) {
        boolean evals = false;
        if (input != null && !input.isEmpty()) {
            channelContext.emit(input);
            evals = evaluates;              // This value gets set synchronously in ChannelHandler
            evaluates = false;              // reset
        }
        return evals;
    }

    public void execute(Tuple tuple, OutputCollector outputCollector) {
        outputCollector.emit(tuple, createValues(tuple));
    }

    private Values createValues(Tuple input) {
        return (Values) input.getValues();
    }

    private class RulesDataSource implements DataSource {
        @Override
        public void open(ChannelContext ctx) {
            SqlStreamEngine.this.channelContext = ctx;
        }
    }

    private class RulesChannelHandler implements ChannelHandler {
        @Override
        public void dataReceived(ChannelContext ctx, Values data) {
            log.debug("Data Received after applying query {}", data);
            SqlStreamEngine.this.evaluates = true;
        }

        @Override
        public void channelInactive(ChannelContext ctx) { }

        @Override
        public void exceptionCaught(Throwable cause) { }
    }

    private class RulesDataSourcesProvider implements DataSourcesProvider {
        @Override
        public String scheme() {
            return SqlStreamExpression.RULE_SCHEMA;
        }

        @Override
        public DataSource construct(URI uri, String s, String s1, List<FieldInfo> list) {
            return SqlStreamEngine.this.dataSource;
        }

        @Override
        public ISqlTridentDataSource constructTrident(URI uri, String s, String s1, List<FieldInfo> list) {
            return null;
        }
    }

    public DataSourcesProvider getDataSourceProvider() {
        return dataSourceProvider;
    }
}
