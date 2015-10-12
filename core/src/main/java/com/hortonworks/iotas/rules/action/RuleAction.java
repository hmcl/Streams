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

package com.hortonworks.iotas.rules.action;

import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.rules.Rule;
import com.hortonworks.iotas.rules.condition.Condition;

import java.util.List;

/** Rule that has part of its execution will invoke another rule or, list of rules.
 *  Multiple rules are evaluated in sequence.
 *
   @param <D> Type of the Design time type declaration of this rule, for example {@link Schema}.
 * @param <I> Type of runtime input to this rule, for example {@code Tuple}
 * @param <F> The type of the first operand in {@link Condition.ConditionElement} of a {@link Condition}, for example {@link Schema.Field}.
 *
 *  */
public interface RuleAction<D, I, F> extends Action<I> {
    /**
     * @return collection of rules that get evaluated during the execution of this action
     */
        List<Rule<D, I, F>> getChainedRules();
}
