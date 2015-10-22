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

import backtype.storm.task.IOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.processor.RulesProcessor;
import com.hortonworks.iotas.layout.design.rule.Rule;
import com.hortonworks.iotas.layout.runtime.processor.ProcessorRuntime;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntime;
import com.hortonworks.rules.condition.GroovyScript;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RulesProcessorRuntimeStorm implements ProcessorRuntime<OutputFieldsDeclarer> {
    public static final Logger logger = LoggerFactory.getLogger(RulesProcessorRuntimeStorm.class);  //TODO

    private RulesProcessor<Schema.Field, Schema.Field, Schema.Field> rulesProcessor;
    private List<RuleRuntime<Tuple, IOutputCollector>> rulesRuntime;

    public RulesProcessorRuntimeStorm(RulesProcessorRuntimeBuilder rulesRuntimeBuilder) {
        //TODO
    }

    public RulesProcessorRuntimeStorm(RulesProcessor<Schema.Field, Schema.Field, Schema.Field> processor) {
        this.rulesProcessor = processor;
        buildAndSetRulesRuntime();             //TODO: Inject this instead
    }

    private void buildAndSetRulesRuntime() {
        final List<Rule<Schema.Field>> rules = rulesProcessor.getRules();
        this.rulesRuntime = new ArrayList<>(rules.size());
        for (Rule<Schema.Field> rule : rules) {
            rulesRuntime.add(new RuleRuntimeStorm(this, rule, new GroovyScript(rule.getCondition())));
        }
    }

    public List<RuleRuntime<Tuple, IOutputCollector>> getRulesRuntime() {
        return rulesRuntime;
    }


    public void declareOutput(OutputFieldsDeclarer declarer) {
        for (Rule<Schema.Field> rule : rulesProcessor.getRules()) {
            declarer.declareStream(getStreamId(rule), getFields(rule));
        }
    }

    String getStreamId(Rule<Schema.Field> rule) {
        return rulesProcessor.getName() + "." + rule.getName();
    }

    Fields getFields(Rule<Schema.Field> rule) {
        final List<Schema.Field> designTimeOutput = rule.getAction().getDeclaredOutput();
        List<String> runtimeFieldNames = new ArrayList<>(designTimeOutput.size());
        for (Schema.Field outputField : designTimeOutput) {
            runtimeFieldNames.add(outputField.getName());
        }
        return new Fields(runtimeFieldNames);
    }

}
