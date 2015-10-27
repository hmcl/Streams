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

import com.hortonworks.iotas.common.Schema.Field;

import java.util.List;

/**
 * This class represents a Rule Condition.
 * @param <F> The type of the first operand in a {@link ConditionElement}, e.g. {@link Field}
 */
public class Condition<F> {
    private List<ConditionElement<F>> conditionElements;

    public Condition() {
        // For JSON serializer
    }

    /** @return The collection of condition elements that define this condition */
    public List<ConditionElement<F>> getConditionElements() {
        return conditionElements;
    }

    public void setConditionElements(List<ConditionElement<F>> conditionElements) {
        this.conditionElements = conditionElements;
    }

    @Override
    public String toString() {
        return "Condition{" +
                "conditionElements=" + conditionElements +
                '}';
    }

    public static class ConditionElement<F> {
        public enum Operation {EQUALS, NOT_EQUAL, GREATER_THAN, LESS_THAN, GREATER_THAN_EQUALS_TO, LESS_THAN_EQUALS_TO}   //TODO: Support BETWEEN ?

        public enum LogicalOperator {AND, OR}

        private F firstOperand;
        private Operation operation;
        private String secondOperand;
        private LogicalOperator logicalOperator;

        public ConditionElement() {
            // For JSON serializer
        }

        /**
         * @return The first operand of this condition
         */
        public F getFirstOperand() {
            return firstOperand;
        }

        
        public void setFirstOperand(F firstOperand) {
            this.firstOperand = firstOperand;
        }

        /**
         * @return The operation applied
         */
        public Operation getOperation() {
            return operation;
        }

        
        public void setOperation(Operation operation) {
            this.operation = operation;
        }

        /**
         * @return The second operand of this condition. It is a constant.
         */
        public String getSecondOperand() {
            return secondOperand;
        }

        
        public void setSecondOperand(String secondOperand) {
            this.secondOperand = secondOperand;
        }

        /**
         * @return The logical operator that precedes the next condition element <br/>
         * null if it is the last condition element of the condition
         */
        public LogicalOperator getLogicalOperator() {
            return logicalOperator;
        }

        public void setLogicalOperator(LogicalOperator logicalOperator) {
            this.logicalOperator = logicalOperator;
        }

        @Override
        public String toString() {
            return "ConditionElement{" +
                    "firstOperand=" + firstOperand +
                    ", operation=" + operation +
                    ", secondOperand='" + secondOperand + '\'' +
                    ", logicalOperator=" + logicalOperator +
                    '}';
        }
    }
}
