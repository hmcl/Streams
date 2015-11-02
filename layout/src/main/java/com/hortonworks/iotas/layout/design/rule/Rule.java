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

package com.hortonworks.iotas.layout.design.rule;

import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.processor.Component;
import com.hortonworks.iotas.layout.design.rule.action.Action;
import com.hortonworks.iotas.layout.design.rule.condition.Condition;

/**
 *
 * @param <O> Type of the design time output declared by this rule's {@link Action}.
 *            This output will become the input of the downstream {@link Component}.Example of output is {@link Schema}
 * @param <F> The type of the first operand in {@link Condition.ConditionElement} of a {@link Condition}, for example {@link Schema.Field}
 */
public class Rule<O, F> {
    private Long id;
    private String name;
    private String description;
    private String ruleProcessorName;

    private Condition<F> condition;
    private Action<O> action;

    public Rule() {     //TODO Check
        // For JSON serializer
    }

    public Rule(Condition<F> condition, Action<O> action) {
        this.condition = condition;
        this.action = action;
    }

    // ====== Metadata =======

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getRuleProcessorName() {
        return ruleProcessorName;
    }

    public void setRuleProcessorName(String ruleProcessorName) {
        this.ruleProcessorName = ruleProcessorName;
    }

    // ====== Design time =======

    public Condition<F> getCondition() {
        return condition;
    }

    public void setCondition(Condition<F> condition) {
        this.condition = condition;
    }

    public Action<O> getAction() {
        return action;
    }

    public void setAction(Action<O> action) {
        this.action = action;
    }
}

