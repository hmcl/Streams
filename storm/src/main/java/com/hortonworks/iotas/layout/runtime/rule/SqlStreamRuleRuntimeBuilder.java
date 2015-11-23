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

package com.hortonworks.iotas.layout.runtime.rule;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import com.hortonworks.iotas.layout.design.rule.Rule;
import com.hortonworks.iotas.layout.runtime.rule.condition.expression.SqlStreamExpression;
import com.hortonworks.iotas.layout.runtime.rule.sql.SqlStreamEngine;
import com.hortonworks.iotas.layout.runtime.rule.sql.SqlStreamScript;

public class SqlStreamRuleRuntimeBuilder implements RuleRuntimeBuilder<Tuple, OutputCollector> {
    private SqlStreamExpression sqlStreamExpression;
    private SqlStreamEngine sqlStreamEngine;
    private SqlStreamScript sqlStreamScript;

    public void buildExpression(Rule rule) {
        sqlStreamExpression = new SqlStreamExpression(rule.getCondition());
    }

    public void buildScriptEngine() {
        sqlStreamEngine = new SqlStreamEngine();
    }

    public void buildScript() {
        sqlStreamScript = new SqlStreamScript(sqlStreamExpression, sqlStreamEngine);
    }

    public RuleRuntimeStorm getRuleRuntime(Rule rule) {
        return new RuleRuntimeStorm(rule, sqlStreamScript);
    }

    @Override
    public String toString() {
        return "SqlStreamRuleRuntimeBuilder{" +
                ", sqlStreamEngine=" + sqlStreamEngine +
                ", sqlStreamScript=" + sqlStreamScript +
                '}';
    }
}
