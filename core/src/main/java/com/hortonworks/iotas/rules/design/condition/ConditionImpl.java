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

package com.hortonworks.iotas.rules.design.condition;

import com.hortonworks.iotas.rules.design.condition.expression.ExpressionBuilder;
import com.hortonworks.iotas.rules.design.condition.script.ScriptExecutor;

import javax.script.ScriptException;
import java.util.Collection;

public class ConditionImpl<I, F, S> implements Condition<I, F, S> {
    private ScriptExecutor<I> scriptExecutor;
    private ExpressionBuilder expressionBuilder;
    private Collection<ConditionElement<F, S>> conditionElements;
    private String conditionString;

    public ConditionImpl(ScriptExecutor<I> scriptExecutor, ExpressionBuilder expressionBuilder) {
        this.scriptExecutor = scriptExecutor;
        this.expressionBuilder = expressionBuilder;
    }

    @Override
    public boolean evaluate(I input) {
        try {
            return scriptExecutor.evaluate(input);
        } catch (ScriptException e) {
            throw new RuntimeException("Exception occurred while evaluating condition.", e);
        }
    }

    @Override
    public void setConditionElements(Collection<ConditionElement<F, S>> conditionElements) {
        this.conditionElements = conditionElements;
    }

    @Override
    public Collection<ConditionElement<F, S>> getConditionElements() {
        return conditionElements;
    }

    @Override
    public String asString() {
        if (conditionString != null) {
            for (ConditionElement conditionElement : conditionElements) {
                conditionString += conditionElement.asString();
            }
        }
        return conditionString;
    }
}
