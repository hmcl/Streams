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

package com.hortonworks.rules.condition.script;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.rule.condition.Condition;
import com.hortonworks.iotas.layout.design.rule.condition.script.Script;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

//TODO
public class GroovyScript extends Script<Tuple, Schema.Field> {
    private final ScriptEngine engine;
    private Bindings bindings;

    private Condition<Schema.Field> condition;
    private String conditionStr;

    public GroovyScript(Condition<Schema.Field> condition) {
        super(condition);
        final ScriptEngineManager factory = new ScriptEngineManager();
        engine = factory.getEngineByName("groovy");
        Bindings bindings = engine.createBindings();
        bindings.put("engine", engine);
    }

    @Override
    public void compile(Condition<Schema.Field> condition) {
        this.condition = condition;
        StringBuilder sb = new StringBuilder("");
        for (Condition.ConditionElement<Schema.Field> element : condition.getConditionElements()) {
//            sb.append(element.getFirstOperand().getType().getJavaType().getSimpleName())
//                    .append(" ")
                    sb.append(element.getFirstOperand().getName())
                    .append(" = ")
                    .append(element.getSecondOperand())     // it is a constant
                    .append(";")
                    .append(" ");
        }
        conditionStr = sb.toString();                       // Integer x = 5;
    }

    @Override
    public boolean evaluate(Tuple input) throws ScriptException {
        Fields fields = input.getFields();
        for (String field : fields) {
            Object val = input.getValueByField(field);
            engine.put(field, val);
        }
        return (boolean) engine.eval(conditionStr);
    }
}
