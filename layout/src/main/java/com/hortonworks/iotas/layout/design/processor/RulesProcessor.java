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

package com.hortonworks.iotas.layout.design.processor;

import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.rule.Rule;
import com.hortonworks.iotas.layout.design.rule.condition.Condition;

import java.util.List;

/**
 * Object representing a design time rules processor.
 * @param <I> Type of the design time input declared by this {@link RulesProcessor}, for example {@link Schema}.
 * @param <O> Type of the design time output declared by this {@link RulesProcessor}, for example {@link Schema}.
 * @param <F> The type of the first operand in {@link Condition.ConditionElement} of a {@link Rule} {@link Condition}, for example {@link Schema.Field}
 */
public class RulesProcessor<I, O, F> extends Component<I> {
    private List<Rule<O,F>> rules;

    public List<Rule<O,F>> getRules() {
        return rules;
    }

    public void setRules(List<Rule<O,F>> rules) {
        this.rules = rules;
    }
}
