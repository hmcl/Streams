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

package com.hortonworks.rules;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import groovy.util.Eval;
import org.junit.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class GroovyTest {
    @Test
    public void testName() throws Exception {
        final ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("groovy");
//        Bindings bindings = engine.createBindings();
//        bindings.put("engine", engine);
//        bindings.put("x", 5);
//        bindings.put("y", 3);

//        System.out.println(engine.getBindings(ScriptContext.GLOBAL_SCOPE));
//        Object record = engine.eval("x > 2 && y > 1");
//        Object record = engine.eval("int x = 5; int y = 3; evaluate(x > 2 && y > 1)");
//        Object record = engine.eval("x > 2 && y > 1");

//        String s = "int x = 5; int y = 3; evaluate(x > 2 && y > 1)";
//        String s = "int x = 5; int y = 3; x > 2 && y > 1";

        engine.put("x", "5");
        engine.put("y", "3");
//        engine.put("x", 5);
//        engine.put("y", 3);
//        String s = "x > 2 && y > 1";
        String s = "x  > '2'  &&  y  > 1";
        Object record = engine.eval(s);
        System.out.printf("evaluating [%s] with (x,y)=(%s,%s) => %s\n", s, engine.get("x"), engine.get("y"), record);

//        engine.put("y", "0");
        engine.put("y", 0);
        record = engine.eval(s);
        System.out.printf("evaluating [%s] with (x,y)=(%s,%s) => %s\n", s, engine.get("x"), engine.get("y"), record);
    }

    @Test
    public void testName1() throws Exception {
        int result = (int) Eval.me("33*3");
        System.out.println(result);
        assert result == 99;
    }

    @Test
    public void testGroovyShell() throws Exception {
        GroovyShell groovyShell = new GroovyShell();
        Binding binding = new Binding();
        binding.setProperty("x",5);
        binding.setProperty("y",3);
        final String s = "x  > 2  &&  y  > 1";
        Script script = groovyShell.parse(s);
        script.setBinding(binding);
        Object result = script.run();
        System.out.printf("evaluating [%s] with (x,y)=(%s,%s) => %s\n", s, script.getBinding().getProperty("x"), script.getBinding().getProperty("y"), result);

        boolean x = binding.hasVariable("x");

//        script.setBinding(null);
        script.setBinding(new Binding());


        /*Binding binding1 = new Binding();
        binding1.setProperty("x",55);
        binding1.setProperty("y",33);
        script.setBinding(binding1);*/
        Object result1 = script.run();
        System.out.printf("evaluating [%s] with (x,y)=(%s,%s) => %s\n", s, script.getBinding().getProperty("x"), script.getBinding().getProperty("y"), result);

        binding.setProperty("y",0);
        result = script.run();
        System.out.printf("evaluating [%s] with (x,y)=(%s,%s) => %s\n", s, binding.getProperty("x"), binding.getProperty("y"), result);

        binding.setProperty("x",null);
        binding.setProperty("y",null);
        binding.setProperty("x1",-1);
        binding.setProperty("y1",-3);
        result = script.run();
        System.out.printf("evaluating [%s] with (x,y)=(%s,%s) => %s\n", s, binding.getProperty("x"), binding.getProperty("y"), result);
        System.out.printf("evaluating [%s] with (x,y,z)=(%s,%s,%s) => %s\n", s, binding.getProperty("x"), binding.getProperty("y"), binding.getProperty("z"), result);
       /*


        def shell = new GroovyShell()

        def b1 = new Binding(x:3)
        def b2 = new Binding(x:4)
        def script = shell.parse('x = 2*x')
        script.binding = b1
        script.run()
        script.binding = b2
        script.run()
        assert b1.getProperty('x') == 6
        assert b2.getProperty('x') == 8
        assert b1 != b2*/




    }
}
