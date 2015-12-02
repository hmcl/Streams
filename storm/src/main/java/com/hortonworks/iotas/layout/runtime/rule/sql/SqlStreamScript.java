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

import backtype.storm.tuple.Values;
import com.hortonworks.iotas.common.IotasEvent;
import com.hortonworks.iotas.layout.runtime.rule.condition.expression.Expression;
import com.hortonworks.iotas.layout.runtime.rule.condition.expression.SqlStreamExpression;
import com.hortonworks.iotas.layout.runtime.rule.condition.script.Script;
import com.hortonworks.iotas.layout.runtime.rule.condition.script.engine.ScriptEngine;

import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.List;

import static com.hortonworks.iotas.layout.runtime.rule.condition.expression.SqlStreamExpression.RULE_SCHEMA;
import static com.hortonworks.iotas.layout.runtime.rule.condition.expression.SqlStreamExpression.RULE_TABLE;

public class SqlStreamScript extends Script<IotasEvent, SqlStreamEngine> {

    public SqlStreamScript(Expression expression,
                           ScriptEngine<SqlStreamEngine> scriptEngine) {
        super(expression, scriptEngine);

        // This is needed to avoid ServiceLoader limitation. Please read comments in RulesDataSourcesProvider
        RulesDataSourcesProvider.setDelegate(((SqlStreamEngine)scriptEngine).getDataSourceProvider());

        ((SqlStreamEngine)scriptEngine).compileQuery(createQuery((SqlStreamExpression) expression));
    }

    private List<String> createQuery(SqlStreamExpression expression) {
        final List<String> statements = new ArrayList<>(2);
        statements.add(expression.createTable(RULE_SCHEMA, RULE_TABLE));
        statements.add(expression.select(RULE_TABLE));
        return statements;
    }

    @Override
    public boolean evaluate(IotasEvent iotasEvent) throws ScriptException {
        final String expressionStr = expression.asString();
        log.debug("Evaluating [{}] with [{}]", expressionStr, iotasEvent);
        boolean evaluates = false;
        if (iotasEvent != null) {
            evaluates = scriptEngine.eval(createValues(iotasEvent));
        }
        log.debug("Expression [{}] evaluated to [{}]", expressionStr, evaluates);
        return evaluates;
    }

    private Values createValues(IotasEvent iotasEvent) {
        final Values values = new Values();
        for (Object value : iotasEvent.getFieldsAndValues().values()) {
            values.add(value);
        }
        return values;
    }
}
