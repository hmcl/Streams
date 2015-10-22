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

package com.hortonworks.iotas.layout.design.rule.condition;

import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.rule.condition.expression.ExpressionBuilder;

import java.util.List;

public class ConditionImpl implements Condition<Schema.Field> {
    private List<ConditionElement<Schema.Field>> conditionElements;
    private String conditionString;

    public ConditionImpl() {
        // For JSON serializer
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
        if (conditionString != null) {      // TODO: Check if I need to cache this
            StringBuilder builder = new StringBuilder("");
            for (ConditionElement conditionElement : conditionElements) {
                builder.append(conditionElement.toString());
            }
            conditionString = builder.toString();
        }
        return conditionString;
    }

    public static class ConditionElementImpl implements ConditionElement<Schema.Field> {
        private Schema.Field firstOperand;    // first operand
        private Operation operation;
        private String secondOperand;         // second operand
        private LogicalOperator logicalOperator;
        private ExpressionBuilder builder;

        private ConditionElementImpl() {
            // For JSON serializer
        }

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
        public Operation getOperation() {
            return operation;
        }

        public void setOperation(Operation operation) {
            this.operation = operation;
        }

        @Override
        public String getSecondOperand() {
            return secondOperand;
        }

        public void setSecondOperand(String secondOperand) {
            this.secondOperand = secondOperand;
        }

        @Override
        public LogicalOperator getLogicalOperator() {
            return logicalOperator;
        }

        public void setLogicalOperator(LogicalOperator logicalOperator) {
            this.logicalOperator = logicalOperator;
        }

        public ExpressionBuilder getBuilder() {
            return builder;
        }

        public void setBuilder(ExpressionBuilder builder) {
            this.builder = builder;
        }

        /** Example of output: temperature > 100 [&&] */
        public String toString() {
            return firstOperand.getName() + " " + operation + " " + secondOperand + " "
                    + (logicalOperator != null ? logicalOperator : "");
        }
    }
}
