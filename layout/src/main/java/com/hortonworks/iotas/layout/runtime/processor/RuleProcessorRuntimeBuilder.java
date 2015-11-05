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
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntime;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntimeBuilder;

/**
 * @param <I> Type of runtime input to each rule, for example {@code Tuple}
 * @param <E> Type of object required to execute each rule in the underlying streaming framework e.g {@code IOutputCollector}
 * @param <O> Type used to declare the output in the the underlying streaming framework,
 *            for example for Apache Storm would be {@code OutputFieldsDeclarer}.
 */
public class RuleProcessorRuntimeBuilder<I, E, O> {
    private final RulesProcessor<I, E, I> rulesProcessor;
    private Class<? extends RuleRuntime<I, E>> ruleRuntimeClass;

    public RuleProcessorRuntimeBuilder(RulesProcessor<I, E, I> rulesProcessor, RuleRuntimeBuilder builder) {


        this.rulesProcessor = rulesProcessor;
    }

    public RuleProcessorRuntime<I,E,O> getRuleProcessorRuntime(RulesProcessor<I, E, I> rulesProcessor, Class<? extends RuleRuntime<I,E>> clazz) {
        /*List<Rule<E, I>> rules = rulesProcessor.getRules();
        List<Object> rulesRuntime = new ArrayList<>(rules.size());
        for (Rule<Schema, Schema.Field> rule : rules) {
            rulesRuntime.add(new RuleRuntimeStorm(rule, new GroovyScript(new GroovyExpressionBuilder<>(rule.getCondition(),
                    new SchemaFieldNameTypeExtractor()), new GroovyScriptEngineBuilder())));      // TODO: Make scripting language pluggable
        }*/
        return null;
    }
}
