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

package com.hortonworks.iotas.rules.design;

import com.hortonworks.iotas.rules.design.action.Action;
import com.hortonworks.iotas.rules.design.condition.Condition;
import com.hortonworks.iotas.rules.design.definition.Definition;

/**
 * @param <I> The type of input to this rule
 */
public interface Rule<I> {
    // ===== Design Time =====
    /** @return the rule definition */
    Definition getDefinition();

    /** @return the condition which when evaluating to true causes this rule's action to execute */
    Condition getCondition();

    /** @return the action that gets executed when this rule's condition evaluates to true */
    Action getAction();

    // ===== Runtime =====

    /** Evaluates Condition
     *  @param input The output of a parser. Key is the field name, value is the field value
     **/
    boolean evaluate(I input);

    /** Executes Action
     *  @param input The output of a parser. Key is the field name, value is the field value
     **/
    void execute(I input);
}
