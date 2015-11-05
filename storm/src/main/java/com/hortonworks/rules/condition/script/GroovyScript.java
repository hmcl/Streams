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
import com.hortonworks.iotas.common.IotasEvent;
import com.hortonworks.iotas.layout.runtime.rule.condition.expression.ExpressionBuilder;
import com.hortonworks.iotas.layout.runtime.rule.condition.script.Script;
import com.hortonworks.iotas.layout.runtime.rule.condition.script.engine.ScriptEngineBuilder;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.Map;

//TODO
public class GroovyScript<F> extends Script<Tuple, F, ScriptEngine> {

    public GroovyScript(ExpressionBuilder<F> expressionBuilder,
                        ScriptEngineBuilder<ScriptEngine> scriptEngineBuilder) {
        super(expressionBuilder, scriptEngineBuilder);
    }

    @Override
    public boolean evaluate(Tuple input) throws ScriptException {
        Object valueByField = input.getValueByField(IotasEvent.IOTAS_EVENT);
        log.debug("valueByField = " + valueByField);
        final IotasEvent iotasEvent = (IotasEvent) input.getValueByField(IotasEvent.IOTAS_EVENT);
        for (Map.Entry<String, Object> fieldAndValue : iotasEvent.getFieldsAndValues().entrySet()) {
            log.debug("PUTTING INTO ENGINE key = {}, val = {}", fieldAndValue.getKey(), fieldAndValue.getValue());
//            engine.put(fieldAndValue.getKey().trim(), fieldAndValue.getValue().toString().trim());
            engine.put(fieldAndValue.getKey(), fieldAndValue.getValue());
        }
        log.debug("Evaluating expression: {}", expression);
        final boolean result = (boolean) engine.eval(expression);
        log.debug("Result = {}",result);
        return result;
    }
}
