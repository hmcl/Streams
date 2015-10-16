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

package com.hortonworks.rules;

import backtype.storm.tuple.Tuple;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.rule.Rule;
import com.hortonworks.iotas.layout.rule.action.Action;
import com.hortonworks.iotas.layout.rule.condition.Condition;
import com.hortonworks.iotas.layout.rule.condition.script.Script;
import com.hortonworks.iotas.layout.rule.exception.ConditionEvaluationException;

import javax.script.ScriptException;

public class RuleImpl implements Rule<Schema, Tuple, Schema.Field> {
    private Long id;
    private String name;
    private String description;

    private Schema declaration;
    private Condition<Schema.Field> condition;
    private Action<Tuple> action;
    private Script<Tuple, Schema.Field> script;     // Script used to evaluate the condition

    public RuleImpl(Condition<Schema.Field> condition, Action<Tuple> action, Script<Tuple, Schema.Field> script) {
        this.condition = condition;
        this.action = action;
        this.script = script;
        compileScript();
    }

    private void compileScript() {
        script.compile(condition);
    }

    // ====== Metadata =======
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
    public Schema getDeclaration() {
        return declaration;
    }

    @Override
    public void setDeclaration(Schema declaration) {
        this.declaration = declaration;
    }

    // ====== Design time =======

    @Override
    public Condition<Schema.Field> getCondition() {
        return condition;
    }

    @Override
    public void setCondition(Condition<Schema.Field> condition) {
        this.condition = condition;
    }

    // ====== Runtime =======

    @Override
    public Script<Tuple, Schema.Field> getScript() {
        return script;
    }

    @Override
    public void setScript(Script<Tuple, Schema.Field> script) {
        this.script = script;
        script.compile(condition);
    }

    @Override
    public Action<Tuple> getAction() {
        return action;
    }

    @Override
    public void setAction(Action<Tuple> action) {
        this.action = action;
    }

    @Override
    public boolean evaluate(Tuple input) {
        try {
            return script.evaluate(input);
        } catch (ScriptException e) {
            throw new ConditionEvaluationException("Exception occurred when evaluating condition: " + this, e);
        }
    }

    @Override
    public void execute(Tuple input) {
        action.execute(input);
    }

    @Override
    public String toString() {
        return "RuleImpl{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", declaration=" + declaration +
                ", condition=" + condition +
                ", action=" + action +
                ", script=" + script +
                '}';
    }

    class C {
        Long l1;
        Long l2;

    void method() {
        if (l1 < l2) {
            System.out.println("xico");
        }
    }
    }
}

