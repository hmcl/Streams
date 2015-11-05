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

package com.hortonworks.rules.runtime;

import backtype.storm.task.IOutputCollector;
import backtype.storm.tuple.Tuple;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.rule.Rule;
import com.hortonworks.iotas.layout.design.rule.action.Action;
import com.hortonworks.iotas.layout.design.rule.condition.Condition;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntime;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntimeBuilder;
import com.hortonworks.iotas.layout.runtime.rule.condition.expression.FieldNameTypeExtractor;
import com.hortonworks.iotas.layout.runtime.rule.condition.expression.GroovyExpressionBuilder;
import com.hortonworks.iotas.layout.runtime.rule.condition.script.engine.GroovyScriptEngineBuilder;
import com.hortonworks.rules.condition.script.GroovyScript;

/**
 * @param <O> Type of the design time output declared by a rule's {@link Action}. Example of output is {@link Schema}
 * @param <F> The type of the first operand in {@link Condition.ConditionElement} of a {@link Condition}, for example {@link Schema.Field}
 */
public class GroovyRuleRuntimeBuilder<O, F> implements RuleRuntimeBuilder<Tuple, IOutputCollector> {
    private FieldNameTypeExtractor<F> fieldNameTypeExtractor;
    private GroovyExpressionBuilder<F> groovyExpressionBuilder;
    private GroovyScriptEngineBuilder groovyScriptEngineBuilder;
    private GroovyScript<F> groovyScript;

    public GroovyRuleRuntimeBuilder(FieldNameTypeExtractor<F> fieldNameTypeExtractor) {
        this.fieldNameTypeExtractor = fieldNameTypeExtractor;
    }

    @Override
    public void buildExpression(Rule rule) {
        groovyExpressionBuilder = new GroovyExpressionBuilder<>(rule.getCondition(), fieldNameTypeExtractor);
    }

    @Override
    public void buildScriptEngine() {
        groovyScriptEngineBuilder = new GroovyScriptEngineBuilder();
    }

    @Override
    public void buildScript() {
        groovyScript = new GroovyScript<>(groovyExpressionBuilder, groovyScriptEngineBuilder);
    }

    @Override
    public RuleRuntime<Tuple, IOutputCollector> getRuleRuntime(Rule rule) {
        return new RuleRuntimeStorm(rule, groovyScript);
    }
}
