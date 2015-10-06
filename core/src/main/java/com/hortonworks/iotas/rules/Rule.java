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

package com.hortonworks.iotas.rules;

import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.rules.action.Action;
import com.hortonworks.iotas.rules.condition.Condition;
import com.hortonworks.iotas.rules.condition.ConditionElement;

/**
 * @param <D> Type of the Design time input to this rule, for example {@link Schema}.
 * @param <I> Type of runtime input to this rule, for example {@code Tuple}
 * @param <F> The type of the first operand in {@link ConditionElement} of a {@link Condition}, for example {@link Schema.Field}.
 */
public interface Rule<D, I, F> {
    // Metadata
    Long getId();

    void setId(Long id);

    String getName();

    void setName(String name);

    String getDescription();

    void setDescription(String description);

    // ===== Design Time =====

    /** @return the rule declaration */
    D getDeclaration();

    void setDeclaration(D declaration);

    /** @return the condition which when evaluating to true causes this rule's action to execute */
    Condition<I, F> getCondition();

    void setCondition(Condition<I, F>  condition);

    /** @return the action that gets executed when this rule's condition evaluates to true */
    Action<I> getAction();

    void setAction(Action<I> action);

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
