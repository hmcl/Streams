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

package com.hortonworks.rules.runtime;

import backtype.storm.task.IOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.component.RulesProcessor;
import com.hortonworks.iotas.layout.design.rule.Rule;
import com.hortonworks.iotas.layout.design.rule.condition.expression.GroovyExpressionBuilder;
import com.hortonworks.iotas.layout.design.rule.condition.expression.SchemaFieldNameTypeExtractor;
import com.hortonworks.iotas.layout.design.rule.condition.script.engine.GroovyScriptEngineBuilder;
import com.hortonworks.iotas.layout.runtime.processor.RuleProcessorRuntime;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntime;
import com.hortonworks.rules.condition.script.GroovyScript;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RulesProcessorRuntimeStorm implements RuleProcessorRuntime<Tuple, IOutputCollector, OutputFieldsDeclarer> {
    public static final Logger log = LoggerFactory.getLogger(RulesProcessorRuntimeStorm.class);  //TODO

    private RulesProcessor<Schema, Schema, Schema.Field> rulesProcessor;
    private List<RuleRuntimeStorm> rulesRuntime;

    /*public RulesProcessorRuntimeStorm(RulesRuntimeStormBuilder<Tuple, IOutputCollector> rulesRuntimeBuilder) {
        rulesRuntime = rulesRuntimeBuilder.getRulesRuntime();
    }*/

    public RulesProcessorRuntimeStorm(List<RuleRuntimeStorm> rulesRuntime) {
        this.rulesRuntime = rulesRuntime;
    }

    public RulesProcessorRuntimeStorm(RulesProcessor<Schema, Schema, Schema.Field> processor) {
        this.rulesProcessor = processor;
        buildAndSetRulesRuntime();             //TODO: Inject this to make it easier to test
    }

    private void buildAndSetRulesRuntime() {
        final List<Rule<Schema, Schema.Field>> rules = rulesProcessor.getRules();
        rulesRuntime = new ArrayList<>(rules.size());
        for (Rule<Schema, Schema.Field> rule : rules) {
            rulesRuntime.add(new RuleRuntimeStorm(rule, new GroovyScript(new GroovyExpressionBuilder<>(rule.getCondition(),
                    new SchemaFieldNameTypeExtractor()), new GroovyScriptEngineBuilder())));      // TODO: Make scripting language pluggable
        }
    }

    @Override
    public List<? extends RuleRuntime<Tuple, IOutputCollector>> getRulesRuntime() {
        return rulesRuntime;
    }

    @Override
    public void setRulesRuntime(List<? extends RuleRuntime<Tuple, IOutputCollector>> rulesRuntime) {
        this.rulesRuntime = (List<RuleRuntimeStorm>) rulesRuntime;
    }

    public void setRulesProcessor(RulesProcessor<Schema, Schema, Schema.Field> rulesProcessor) {
        this.rulesProcessor = rulesProcessor;
    }

    public void declareOutput(OutputFieldsDeclarer declarer) {
        for (RuleRuntimeStorm ruleRuntime:rulesRuntime) {
            ruleRuntime.declareOutput(declarer);
        }
    }
}

