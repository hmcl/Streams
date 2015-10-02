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

package com.hortonworks.iotas.common;

import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class GroovyTest {
    @Test
    public void testName() throws Exception {
        final ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("Nashorn");
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
