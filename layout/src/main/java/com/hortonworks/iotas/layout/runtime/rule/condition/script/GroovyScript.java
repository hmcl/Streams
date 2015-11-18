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

package com.hortonworks.iotas.layout.runtime.rule.condition.script;

import com.hortonworks.iotas.common.IotasEvent;
import com.hortonworks.iotas.layout.runtime.rule.condition.expression.Expression;
import com.hortonworks.iotas.layout.runtime.rule.condition.script.engine.ScriptEngine;
import groovy.lang.Binding;

import javax.script.ScriptException;
import java.util.Map;

//TODO
public class GroovyScript extends Script<IotasEvent, groovy.lang.Script> {
    private static final Binding EMPTY_BINDING = new Binding();

    public GroovyScript(Expression expression,
                        ScriptEngine<groovy.lang.Script> scriptEngine) {
        super(expression, scriptEngine);
        log.debug("Created Groovy Script: {}", super.toString());
    }

    @Override
    public boolean evaluate(IotasEvent iotasEvent) throws ScriptException {
        log.debug("Evaluating [{}] with [{}]", expression, iotasEvent);
        boolean evaluates = false;
        try {
            if (iotasEvent != null) {
                final Map<String, Object> fieldsToValues = iotasEvent.getFieldsAndValues();
                scriptEngine.setBinding(new Binding(fieldsToValues));
                log.debug("Set script binding to [{}]", fieldsToValues);
                evaluates = (boolean) scriptEngine.run();
                log.debug("Expression [{}] evaluated to [{}]", expression, evaluates);
            }
        } catch (groovy.lang.MissingPropertyException e) {
            // Occurs when not all the properties required for evaluating the script are set. This can happen for example
            // when receiving an IotasEvent that does not have all the fields required to evaluate the expression
            log.debug("Missing property required to evaluate expression.", e);
            evaluates = false;
        }
        finally {
            scriptEngine.setBinding(EMPTY_BINDING);
            log.debug("Script binding reset to empty binding");
        }
        return evaluates;
    }
}
