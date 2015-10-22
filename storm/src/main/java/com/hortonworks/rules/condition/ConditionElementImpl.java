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

package com.hortonworks.rules.condition;

import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.rule.condition.Condition;
import com.hortonworks.iotas.layout.design.rule.condition.expression.ExpressionBuilder;

public abstract class ConditionElementImpl<F, V> implements Condition.ConditionElement<Schema.Field> {
    private Schema.Field firstOperand;    // first operand
    private String secondOperand;         // second operand
    private LogicalOperator logicalOperator;
    private Operation operation;
    private ExpressionBuilder builder;

    public ConditionElementImpl(ExpressionBuilder builder) {
        this.builder = builder;
    }


    @Override
    public Schema.Field getFirstOperand() {
        return firstOperand;
    }

    public void setFirstOperand(Schema.Field firstOperand) {
        this.firstOperand = firstOperand;
    }

    @Override
    public String getSecondOperand() {
        return secondOperand;
    }

    public void setSecondOperand(String secondOperand) {
        this.secondOperand = secondOperand;
    }

    public ExpressionBuilder getBuilder() {
        return builder;
    }

    @Override
    public LogicalOperator getLogicalOperator() {
        return logicalOperator;
    }

    public void setLogicalOperator(LogicalOperator logicalOperator) {
        this.logicalOperator = logicalOperator;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    /** Example of output: temperature > 100 [&&] */
    public String toString() {
        return firstOperand.getName() + getOperation() + secondOperand + logicalOperator ;
    }
}
