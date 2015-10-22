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

package com.hortonworks.iotas.layout.design.rule.condition.expression;

import com.hortonworks.iotas.layout.design.rule.condition.Condition;

/** Translates the DSL expression into the implementation language syntax */
public interface ExpressionBuilder {
    /**
     * @param operator the DSL logicalOperator for which to obtain the operator syntax
     * @return the implementation language operator syntax
     */
    String getLogicalOperator(Condition.ConditionElement.LogicalOperator operator);

    /**
     * @param operation the DSL Operation for which to obtain the operation syntax
     * @return the implementation language operation syntax
     */
    String getOperation(Condition.ConditionElement.Operation operation);
}
