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

package com.hortonworks.iotas.layout.design.rule.condition.script;

import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.rule.condition.expression.ExpressionBuilder;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * @param <I> The type of input on which this script is evaluated, e.g. {@code tuple}
 * @param <F> The name and type declaration of the fields that constitute the Condition to be evaluated e.g. {@link Schema.Field}
 */
public abstract class AbstractGroovyScript<I,F> extends Script<I,F> {
    protected ScriptEngine engine;
    protected Bindings bindings;

    public AbstractGroovyScript(ExpressionBuilder<F> expression) {
        super(expression);
        setupScriptEngine();
    }

    private void setupScriptEngine() {
        final ScriptEngineManager factory = new ScriptEngineManager();
        engine = factory.getEngineByName("groovy");
        Bindings bindings = engine.createBindings();
        bindings.put("engine", engine);
    }

    @Override
    public abstract void compile(ExpressionBuilder<F> expression);

    @Override
    public abstract boolean evaluate(I input) throws ScriptException;
}
