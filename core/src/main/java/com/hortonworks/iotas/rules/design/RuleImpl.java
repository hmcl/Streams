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

import backtype.storm.tuple.Tuple;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.rules.design.action.Action;
import com.hortonworks.iotas.rules.design.condition.Condition;

public class RuleImpl implements Rule<Schema, Tuple, Schema.Field> {
    private Long id;
    private String name;
    private String description;

    private Schema declaration;
    private Condition<Tuple, Schema.Field> condition;
    private Action<Tuple> action;

    public RuleImpl(Condition<Tuple, Schema.Field> condition, Action<Tuple> action) {
        this.condition = condition;
        this.action = action;
    }

    @Override
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public D getDeclaration() {
        return declaration;
    }

    @Override
    public void setDeclaration(D declaration) {
        this.declaration = declaration;
    }

    @Override
    public Condition<I, F> getCondition() {
        return condition;
    }

    @Override
    public void setCondition(Condition<I, F> condition) {
        this.condition = condition;
    }

    @Override
    public Action<I> getAction() {
        return action;
    }

    @Override
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

