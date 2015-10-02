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

package com.hortonworks.iotas.rules.condition;

import com.hortonworks.iotas.rules.condition.expression.ExpressionBuilder;

import java.util.Collection;

public class ConditionImpl implements Condition {
    private ScriptExecutor scriptExecutor;
    private ExpressionBuilder expressionBuilder;
    private Collection<ConditionElement> conditionElements;
    private String condString;

    public ConditionImpl(ScriptExecutor scriptExecutor, ExpressionBuilder expressionBuilder) {
        this.scriptExecutor = scriptExecutor;
        this.expressionBuilder = expressionBuilder;
    }

    @Override
    public boolean evaluate() {
        return scriptExecutor.evaluate(this);
    }

    @Override
    public void setConditionElements(Collection<ConditionElement> conditionElements) {
        this.conditionElements = conditionElements;
    }

    @Override
    public Collection<ConditionElement> getConditionElements() {
        return conditionElements;
    }

    @Override
    public String getConditionString() {
        if (condString != null) {
            for (ConditionElement conditionElement : conditionElements) {
                condString += expressionBuilder.getOperation(conditionElement.getOperation());
                condString += expressionBuilder.getLogicalOperator(conditionElement.getLogicalOperator());
            }
        }
        return condString;
    }
}
