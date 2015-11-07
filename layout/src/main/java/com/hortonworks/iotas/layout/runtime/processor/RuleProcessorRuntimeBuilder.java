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

package com.hortonworks.iotas.layout.runtime.processor;

import com.hortonworks.iotas.layout.design.component.RulesProcessor;
import com.hortonworks.iotas.layout.design.rule.Rule;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntime;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntimeBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * @param <I> Type of runtime input to this rule, for example {@code Tuple}
 * @param <E> Type of object required to execute this rule in the underlying streaming framework e.g {@code IOutputCollector}
 * @param <O> Type used to declare the output in the the underlying streaming framework,
 *            for example for Apache Storm would be {@code OutputFieldsDeclarer}.
 */
public abstract class RuleProcessorRuntimeBuilder<I, E, O> {
    protected final RulesProcessor rulesProcessor;
    protected final RuleRuntimeBuilder<I, E, O> ruleRuntimeBuilder;
    protected List<RuleRuntime<I, E, O>> rulesRuntime;

    public RuleProcessorRuntimeBuilder(RulesProcessor rulesProcessor, RuleRuntimeBuilder<I, E, O> ruleRuntimeBuilder) {
        this.rulesProcessor = rulesProcessor;
        this.ruleRuntimeBuilder = ruleRuntimeBuilder;
    }

    //TODO: check null pointers, etc
    public void build() {
        final List<Rule> rules = rulesProcessor.getRules();

        rulesRuntime = new ArrayList<>();

        if (rules != null) {
            for (Rule rule : rules) {
                ruleRuntimeBuilder.buildExpression(rule);
                ruleRuntimeBuilder.buildScriptEngine();
                ruleRuntimeBuilder.buildScript();
                ruleRuntimeBuilder.buildExpression(rule);
                RuleRuntime<I, E, O> ruleRuntime = ruleRuntimeBuilder.getRuleRuntime(rule);
                //TODO: Log
                rulesRuntime.add(ruleRuntime);
            }
        }
    }

    public abstract RuleProcessorRuntime getRuleProcessorRuntime();
}
