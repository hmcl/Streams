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

package com.hortonworks.iotas.layout.processor;

import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.rule.Rule;

import java.util.List;

public class RulesProcessorImpl implements RulesProcessor<Schema.Field, Schema.Field, Schema.Field> {
    private Long id;
    private String name;
    private String description;
    private Schema.Field declaredInput;
    private Schema.Field declaredOutput;
    private List<Rule<Schema.Field>> rules;

    @Override
    public List<Rule<Schema.Field>> getRules() {
        return rules;
    }

    @Override
    public void setRules(List<Rule<Schema.Field>> rules) {
        this.rules = rules;
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
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
    public Schema.Field getDeclaredInput() {
        return declaredInput;
    }

    @Override
    public void setDeclaredInput(Schema.Field declaredInput) {
        this.declaredInput = declaredInput;
    }

    @Override
    public Schema.Field getDeclaredOutput() {
        return declaredOutput;
    }

    @Override
    public void setDeclaredOutput(Schema.Field declaredOutput) {
        this.declaredOutput = declaredOutput;
    }
}
