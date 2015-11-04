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

import com.hortonworks.iotas.layout.design.rule.Rule;
import com.hortonworks.iotas.layout.design.rule.condition.expression.FieldNameTypeExtractor;
import com.hortonworks.iotas.layout.design.rule.condition.expression.GroovyExpressionBuilder;
import com.hortonworks.iotas.layout.design.rule.condition.script.engine.GroovyScriptEngineBuilder;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntime;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntimeBuilder;
import com.hortonworks.rules.condition.script.GroovyScript;

public class GroovyRuleRuntimeBuilder implements RuleRuntimeBuilder {
    private Rule rule;
    private FieldNameTypeExtractor fieldNameTypeExtractor;
    private GroovyExpressionBuilder groovyExpressionBuilder;
    private GroovyScriptEngineBuilder groovyScriptEngineBuilder;
    private GroovyScript groovyScript;

    public GroovyRuleRuntimeBuilder(Rule rule, FieldNameTypeExtractor fieldNameTypeExtractor) {
        this.rule = rule;
        this.fieldNameTypeExtractor = fieldNameTypeExtractor;
    }

    @Override
    public void buildExpression() {
        groovyExpressionBuilder = new GroovyExpressionBuilder<>(rule.getCondition(), fieldNameTypeExtractor);

    }

    @Override
    public void buildScriptEngine() {
        groovyScriptEngineBuilder = new GroovyScriptEngineBuilder();
    }

    @Override
    public void buildScript() {
        groovyScript = new GroovyScript(groovyExpressionBuilder, groovyScriptEngineBuilder);
    }

    @Override
    public RuleRuntime getRuleRuntime() {
        return new RuleRuntimeStorm(rule, groovyScript);
    }
}
