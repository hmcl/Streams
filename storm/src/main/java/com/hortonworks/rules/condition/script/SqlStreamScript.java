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

import backtype.storm.tuple.Tuple;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.rule.condition.Condition;
import com.hortonworks.iotas.layout.design.rule.condition.script.Script;

import javax.script.ScriptException;

// TODO
public class SqlStreamScript extends Script<Tuple, Schema.Field> {
    private Object framework;

    public SqlStreamScript(Condition<Schema.Field> condition, Object framework) {
        super(condition);
        this.framework = framework;
    }

    @Override
    public void compile(Condition condition) {
//        framework.compile(condition);
    }

    @Override
    public boolean evaluate(Tuple input) throws ScriptException {
//        return framework.eval(input);
        return false;
    }
    /*public SqlStreamScript() {
        Interface:

        *//*public interface Evaluation {
            bool	filter(Tuple record);
        }

        Webserver side code:

        Compiler comp = new Compiler(); // From Haohui's class
        Evaluation obj = comp.compile("let x = 1:Integer,...; x + y > 0 and 1 < 2");
        for (Tuple r : record) {
            if (obj.filter(r)) {
                action();
            }
        }

        *//*
    }*/
}
