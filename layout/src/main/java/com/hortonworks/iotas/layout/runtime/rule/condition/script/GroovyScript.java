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

import javax.script.ScriptException;
import java.util.Map;

//TODO
public class GroovyScript<F> extends Script<IotasEvent, F, javax.script.ScriptEngine> {

    public GroovyScript(Expression<F> expression,
                        ScriptEngine<javax.script.ScriptEngine> scriptEngine) {
        super(expression, scriptEngine);
    }

    @Override
    public boolean evaluate(IotasEvent iotasEvent) throws ScriptException {
        for (Map.Entry<String, Object> fieldAndValue : iotasEvent.getFieldsAndValues().entrySet()) {
            log.debug("PUTTING INTO ENGINE key = {}, val = {}", fieldAndValue.getKey(), fieldAndValue.getValue());
//            scriptEngine.put(fieldAndValue.getKey().trim(), fieldAndValue.getValue().toString().trim());
            scriptEngine.put(fieldAndValue.getKey(), fieldAndValue.getValue());
        }
        log.debug("Evaluating expression: {}", expression);
        final boolean result = (boolean) scriptEngine.eval(expression);
        log.debug("Result = {}",result);
        return result;
    }
}