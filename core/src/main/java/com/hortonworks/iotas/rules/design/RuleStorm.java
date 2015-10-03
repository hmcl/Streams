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
import com.hortonworks.iotas.rules.design.action.Action;
import com.hortonworks.iotas.rules.design.condition.Condition;
import com.hortonworks.iotas.rules.design.definition.Definition;

public class RuleStorm implements Rule<Tuple> {
    private Definition definition;
    private Condition condition;
    private Action action;

    public RuleStorm(Definition definition, Condition condition, Action action) {
        this.definition = definition;
        this.condition = condition;
        this.action = action;
    }

    @Override
    public Definition getDefinition() {
        return definition;
    }

    public void setDefinition(Definition definition) {
        this.definition = definition;
    }

    @Override
    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    @Override
    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }


    @Override
    public boolean evaluate(Tuple input) {
        return condition.evaluate(input);
    }

    @Override
    public void execute(Tuple input) {
        action.execute(input);
    }
}

