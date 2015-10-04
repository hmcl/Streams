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

import com.hortonworks.iotas.common.Schema.Type;
import com.hortonworks.iotas.rules.design.condition.expression.ExpressionBuilder;

/**
 * @param <T> type of the first operand, e.g. {@link Type}
 * @param <V> type of the second operand, which should be constant, e.g. {@link String}, {@link Integer}
 */
public interface ConditionElement<T, V> {
    enum Operation {EQUALS, NOT_EQUAL, GREATER_THAN, LESS_THAN, GREATER_THAN_EQUALS_TO, LESS_THAN_EQUALS_TO}   //TODO: Support BETWEEN

    enum LogicalOperator {AND, OR}

    /**
     * @return The first operand of this condition
     */
    FirstOperand<T> getFirstOperand();

    /**
     * @return The second operand of this condition
     */
    SecondOperand<V> getSecondOperand();

    Operation getOperation();

    /**
     * @return The logical operator that precedes the next condition element <br/>
     * null if this is the last condition element of the condition
     */
    LogicalOperator getLogicalOperator();

    ExpressionBuilder getExpressionBuilder();

    String asString();

    interface FirstOperand<T> {
        String getName();
        T getType();
    }

    interface SecondOperand<T> {
        T getValue();
    }

}
