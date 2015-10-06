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

import backtype.storm.tuple.Tuple;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.rules.condition.Condition;
import com.hortonworks.iotas.rules.condition.ConditionElement;
import com.hortonworks.iotas.rules.condition.script.Script;

import javax.script.ScriptException;
import java.util.List;

public class ConditionImpl implements Condition<Tuple, Schema.Field> {
    private Script<Tuple> script;
    private List<ConditionElement<Schema.Field>> conditionElements;
    private String conditionString;

    public ConditionImpl(Script<Tuple> script) {
        this.script = script;
    }

    @Override
    public boolean evaluate(Tuple input) {
        try {
            return script.evaluate(input);
        } catch (ScriptException e) {
            throw new RuntimeException("Exception occurred while evaluating condition.", e);
        }
    }

    @Override
    public void setConditionElements(List<ConditionElement<Schema.Field>> conditionElements) {
        this.conditionElements = conditionElements;
    }

    @Override
    public List<ConditionElement<Schema.Field>> getConditionElements() {
        return conditionElements;
    }

    public String toString() {
        if (conditionString != null) {
            for (ConditionElement conditionElement : conditionElements) {
                conditionString += conditionElement.toString();
            }
        }
        return conditionString;
    }
}
