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

public class ConditionElementImpl<T, V> implements ConditionElement<T, V> {
    private FirstOperand<T> firstOperand;    // first operand
    private SecondOperand<V> secondOperand;   // second operand
    private LogicalOperator logicalOperator;
    private Operation operation;
    private ExpressionBuilder builder;

    private String asString;

    public ConditionElementImpl(ExpressionBuilder builder) {
        this.builder = builder;
    }

    @Override
    public FirstOperand<T> getFirstOperand() {
        return firstOperand;
    }

    public void setFirstOperand(FirstOperand<T> firstOperand) {
        this.firstOperand = firstOperand;
    }

    @Override
    public SecondOperand<V> getSecondOperand() {
        return secondOperand;
    }

    public void setSecondOperand(SecondOperand<V> secondOperand) {
        this.secondOperand = secondOperand;
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

    @Override
    public ExpressionBuilder getExpressionBuilder() {
        return builder;
    }

    public void setBuilder(ExpressionBuilder builder) {
        this.builder = builder;
    }

    /** temperature > 100 */
    public String asString() {
        if (asString != null) {
            asString += firstOperand.getName() + logicalOperator + secondOperand.getValue();
        }
        return asString;
    }
}
