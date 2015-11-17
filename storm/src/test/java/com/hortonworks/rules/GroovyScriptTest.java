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

import com.hortonworks.iotas.common.IotasEventImpl;
import com.hortonworks.iotas.layout.design.rule.condition.Condition;
import com.hortonworks.iotas.layout.runtime.rule.condition.expression.Expression;
import com.hortonworks.iotas.layout.runtime.rule.condition.script.GroovyScript;
import com.hortonworks.iotas.layout.runtime.rule.condition.script.engine.ScriptEngine;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Tested;
import mockit.integration.junit4.JMockit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;

@RunWith(JMockit.class)
public class GroovyScriptTest {
    private static final IotasEventImpl IOTAS_EVENT = new IotasEventImpl(new HashMap<String, Object>() {{
        put("F1", 3);
        put("F2", 4);
    }}, "dataSrcId_1", "id_1");

    private @Tested GroovyScript groovyScript;
    private @Injectable Expression expression;
//    private @Injectable String expression;
    private @Injectable ScriptEngine<javax.script.ScriptEngine> scriptEngine;
    private @Mocked Condition condition;

    @Before
    public void setUp() throws Exception {
//        expression = "F1 > 1 && F2 > 2";

//        expression = new GroovyExpression(condition);
//        scriptEngine = new GroovyScriptEngine();
    }

    @Test
    public void test_2fields_and_condition_matches_true() throws Exception {
        final String twoFieldsAndCondition = "F1 > 1 && F2 > 2";
        new ExpressionExpectation(twoFieldsAndCondition);

        System.out.println("Asserting");
        Assert.assertTrue(groovyScript.evaluate(IOTAS_EVENT));
    }

    private class ExpressionExpectation extends Expectations {
        private String resultStr;

        public ExpressionExpectation(String result) {
            System.out.println("constructor");
            resultStr = result;
        }

        {
            expression.getExpression(); result = "F1 > 1 && F2 > 2";    // F1 - Field1.getName()
//            expression.getExpression(); result = resultStr;    // F1 - Field1.getName()
            System.out.println("static block");
        }

    }




}
