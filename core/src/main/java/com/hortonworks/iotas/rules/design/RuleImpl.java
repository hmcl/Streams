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

public class RuleImpl<D, I> implements Rule<D, I> {
    private Definition<D> definition;
    private Condition<I> condition;
    private Action<I> action;

    public RuleImpl(Definition<D> definition, Condition<I, F , S> condition, Action<I> action) {
        this.definition = definition;
        this.condition = condition;
        this.action = action;
    }

    @Override
    public Definition<D> getDefinition() {
        return definition;
    }

    public void setDefinition(Definition<D> definition) {
        this.definition = definition;
    }

    @Override
    public Condition<I> getCondition() {
        return condition;
    }

    public void setCondition(Condition<I> condition) {
        this.condition = condition;
    }

    @Override
    public Action<I> getAction() {
        return action;
    }

    public void setAction(Action<I> action) {
        this.action = action;
    }


    @Override
    public boolean evaluate(I input) {
        return condition.evaluate(input);
    }

    @Override
    public void execute(I input) {
        action.execute(input);
    }
}

