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

package com.hortonworks.iotas.rules.design.condition.expression;

import com.hortonworks.iotas.rules.design.condition.ConditionElement;

import java.util.Arrays;

public class SqlStreamExpressionBuilder implements ExpressionBuilder {
    @Override
    public String getLogicalOperator(ConditionElement.LogicalOperator operator) {
        switch(operator) {
            case AND:
                return " AND ";
            case OR:
                return " OR ";
            default:
                throw new UnsupportedOperationException(String.format("Operation [%s] not support. List of supported operations: %s",
                        operator, Arrays.toString(ConditionElement.LogicalOperator.values())));
        }
    }

    @Override
    public String getOperation(ConditionElement.Operation operation) {
        switch(operation) {
            case EQUALS:
                return " == ";
            case NOT_EQUAL:
                return " != ";
            case GREATER_THAN:
                return " > ";
            case LESS_THAN:
                return " < ";
            case GREATER_THAN_EQUALS_TO:
                return " >= ";
            case LESS_THAN_EQUALS_TO:
                return " <= ";
            default:
                throw new UnsupportedOperationException(String.format("Operation [%s] not support. List of supported operations: %s",
                        operation, Arrays.toString(ConditionElement.Operation.values())));
        }
    }
}
