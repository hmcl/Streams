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

package com.hortonworks.bolt;

import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.hortonworks.iotas.layout.design.processor.RulesProcessor;
import com.hortonworks.iotas.layout.design.rule.Rule;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntime;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntimeBuilder;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntimeConstructor;
import com.hortonworks.rules.runtime.RuleRuntimeStorm;
import com.hortonworks.rules.runtime.RulesProcessorRuntimeStorm;

import java.util.List;
import java.util.Map;

public class RulesBolt<I, O, F> extends BaseRichBolt {
    private final RulesProcessor<I, O, F> rulesProcessor;
    private final RuleRuntimeBuilder<Tuple, IOutputCollector> ruleRuntimeBuilder;
    private OutputCollector collector;
    //TODO Clean thse two vars
    private List<RuleRuntime<Tuple, IOutputCollector>> rulesRuntime;
    private RulesProcessorRuntimeStorm rulesProcessorRuntime;

    public RulesBolt(RulesProcessor<I, O, F> rulesProcessor, RuleRuntimeBuilder<Tuple, IOutputCollector> ruleRuntimeBuilder) {
        this.rulesProcessor = rulesProcessor;
        this.ruleRuntimeBuilder = ruleRuntimeBuilder;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        buildRulesRuntime();
    }

    private void buildRulesRuntime() {
        final RuleRuntimeConstructor<Tuple, IOutputCollector> ruleRuntimeConstructor
                = new RuleRuntimeConstructor<>(ruleRuntimeBuilder);
        for (Rule<O,F> rule : rulesProcessor.getRules()) {
            ruleRuntimeConstructor.construct();
            rulesRuntime.add(ruleRuntimeConstructor.getRuleRuntime(rule));
        }

    }

    @Override
    public void execute(Tuple input) {  // tuple input should an IotasEvent
        try {
            for (RuleRuntime<Tuple, IOutputCollector> rule : rulesRuntime) {
                if (rule.evaluate(input)) {
                    rule.execute(input, collector); // collector can be null when the rule does not forward a stream
                }
            }
            collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
            collector.reportError(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //TODO: Clean and how to avoid this cast
//        rulesProcessorRuntime.declareOutput(declarer);

        for (RuleRuntime<Tuple, IOutputCollector> ruleRuntime : rulesRuntime) {
            ((RuleRuntimeStorm)ruleRuntime).declareOutput(declarer);
        }

    }
}