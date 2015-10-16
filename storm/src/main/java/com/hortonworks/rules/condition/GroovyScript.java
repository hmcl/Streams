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

package com.hortonworks.rules.condition;

import backtype.storm.tuple.Tuple;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.rule.condition.Condition;
import com.hortonworks.iotas.layout.rule.condition.script.Script;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

//TODO
public class GroovyScript implements Script<Tuple, Schema.Field> {
    private Condition<Schema.Field> condition;

    @Override
    public void compile(Condition<Schema.Field> condition) {
        this.condition = condition;
    }

    @Override
    public boolean evaluate(Tuple input) throws ScriptException {
        return false;
    }

    private final Bindings bindings;
    private final ScriptEngine engine;

    public GroovyScript() {
        String s = "int x = 5; int y = 3; x > 2 && y > 1";

        final ScriptEngineManager factory = new ScriptEngineManager();
        engine = factory.getEngineByName( "Groovy" );
        bindings = engine.createBindings();
        bindings.put("engine", engine);
    }

    /*@Override
    public boolean evaluate(Condition condition) throws ScriptException {
        return (boolean) engine.eval(condition.asString());
    }*/

    public static void main(String[] args) throws ScriptException {
            final ScriptEngineManager factory = new ScriptEngineManager();
            ScriptEngine engine = factory.getEngineByName("groovy");
            Bindings bindings = engine.createBindings();
            bindings.put("engine", engine);
            bindings.put("x", 5);
            bindings.put("y", 3);
            System.out.println(engine.getBindings(ScriptContext.GLOBAL_SCOPE));
//        Object record = engine.eval("x > 2 && y > 1");
//        Object record = engine.eval("int x = 5; int y = 3; evaluate(x > 2 && y > 1)");
            Object record = engine.eval("x > 2 && y > 1");

            String s = "int x = 5; int y = 3; evaluate(x > 2 && y > 1)";
            System.out.printf("evaluating [%s] => %s", s, record);
    }
}
