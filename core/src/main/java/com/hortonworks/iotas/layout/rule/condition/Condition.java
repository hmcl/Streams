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

package com.hortonworks.iotas.layout.rule.condition;

import com.hortonworks.iotas.common.Schema.Field;

import java.util.List;

/**
 * This class represents a Rule Condition.
 * @param <F> The type of the first operand in a {@link ConditionElement}, e.g. {@link Field}
 */
public interface Condition<F> {
    /** @return The collection of condition elements that define this condition */
    List<ConditionElement<F>> getConditionElements();

    void setConditionElements(List<ConditionElement<F>> conditionElements);

    /**
     * All implementations must override {@code toString} method
     * @return The string representation of this condition as it is evaluated by the script language */
    String toString();

    /**
     * @param <F> type of the first operand, e.g. {@link Field}
     */
    interface ConditionElement<F> {
        enum Operation {EQUALS, NOT_EQUAL, GREATER_THAN, LESS_THAN, GREATER_THAN_EQUALS_TO, LESS_THAN_EQUALS_TO}   //TODO: Support BETWEEN ?

        enum LogicalOperator {AND, OR}

        /**
         * @return The first operand of this condition
         */
        F getFirstOperand();

        /**
         * @return The second operand of this condition. It is a constant
         */
        String getSecondOperand();

        Operation getOperation();

        /**
         * @return The logical operator that precedes the next condition element <br/>
         * null if this is the last condition element of the condition
         */
        LogicalOperator getLogicalOperator();

        /**
         * Every class must implement this method
         */
        String toString();
    }
}
