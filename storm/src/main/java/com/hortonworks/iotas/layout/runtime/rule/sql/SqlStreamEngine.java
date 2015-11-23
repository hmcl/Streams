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
import com.hortonworks.iotas.layout.runtime.rule.condition.script.engine.ScriptEngine;
import org.apache.storm.sql.DataSourcesProvider;
import org.apache.storm.sql.DataSourcesRegistry;
import org.apache.storm.sql.StormSql;
import org.apache.storm.sql.runtime.ChannelContext;
import org.apache.storm.sql.runtime.ChannelHandler;
import org.apache.storm.sql.runtime.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        this.dataSource = this.new RulesDataSource();          // Step 1 && Step 2 - RulesDataSource Sets Channel Context
        this.channelHandler = this.new RulesChannelHandler();
        this.dataSourceProvider = this.new RulesDataSourcesProvider();
        compileQuery();
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

    private void compileQuery() {
        try {
            //TODO: Change API to register data source provider
            DataSourcesRegistry.providerMap().put("RBTS", dataSourceProvider);                 // RBTS - Rules Bolt Table Schema
            List<String> stmnt = new ArrayList<>();
            stmnt.add("CREATE EXTERNAL TABLE RBT (F1 INTEGER, F2 INT, F3 INT) LOCATION 'RBTS:///RBT'");
            stmnt.add("SELECT F1,F2,F3 FROM RBT WHERE F1 < 2 AND F2 < 3 AND F3 < 4");
            StormSql stormSql = StormSql.construct();
            stormSql.execute(stmnt, channelHandler);
        } catch (Exception e) {
            throw new RuntimeException("Failed preparing query", e);
        }
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
            return "RBTS";
        }

        @Override
        public DataSource construct(URI uri, String inputFormatClass,
                String outputFormatClass, List<Map.Entry<String, Class<?>>> fields) {
            return SqlStreamEngine.this.dataSource;
        }
    }

}