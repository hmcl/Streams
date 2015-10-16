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

package com.hortonworks.iotas.layout.rule;

import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.rule.action.Action;
import com.hortonworks.iotas.layout.rule.condition.Condition;

/**
 * @param <F> The type of the first operand in {@link Condition.ConditionElement} of a {@link Condition}, for example {@link Schema.Field}
 */
public interface Rule<F> {
    // Metadata
    Long getId();

    void setId(Long id);

    String getName();

    void setName(String name);

    String getDescription();

    void setDescription(String description);

    // ===== Design Time =====

    /** @return the condition which when evaluating to true causes this rule's action to execute */
    Condition<F> getCondition();

    void setCondition(Condition<F>  condition);

    /** @return the action that gets executed when this rule's condition evaluates to true */
    Action getAction();

    void setAction(Action action);
}
