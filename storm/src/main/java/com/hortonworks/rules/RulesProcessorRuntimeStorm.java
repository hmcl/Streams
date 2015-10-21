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
import com.hortonworks.iotas.layout.processor.RulesProcessor;
import com.hortonworks.iotas.layout.rule.Rule;
import com.hortonworks.iotas.layout.rule.runtime.RuleRuntime;
import com.hortonworks.rules.condition.GroovyScript;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RulesProcessorRuntimeStorm {
    public static final Logger logger = LoggerFactory.getLogger(RulesProcessorRuntimeStorm.class);

    private RulesProcessor<Schema.Field> processor;
    private List<RuleRuntime<Tuple, IOutputCollector>> rulesRuntime;

    public RulesProcessorRuntimeStorm(RulesProcessor<Schema.Field> processor) {
        this.processor = processor;
        buildRuleRuntime();
    }

    private void buildRuleRuntime() {
        final List<Rule<Schema.Field>> rules = processor.getRules();
        this.rulesRuntime = new ArrayList<>(rules.size());
        for (Rule<Schema.Field> rule : rules) {
            rulesRuntime.add(new StormRuleRuntime(rule, new GroovyScript()));
        }
    }

    public List<RuleRuntime<Tuple, IOutputCollector>> getRulesRuntime() {
        return rulesRuntime;
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Rule<Schema.Field> rule : processor.getRules()) {
            declarer.declareStream(getStreamId(rule), getFields(rule));
        }
    }

    private String getStreamId(Rule<Schema.Field> rule) {
        return processor.getName() + "." + rule.getName();
    }

    private Fields getFields(Rule<Schema.Field> rule) {
        List<Schema.Field> designTimeOutput = rule.getAction().getDeclaredOutput();
        List<String> runtimeFieldNames = new ArrayList<>(designTimeOutput.size());
        for (Schema.Field designTimeField : designTimeOutput) {
            runtimeFieldNames.add(designTimeField.getName());
        }
        return new Fields(runtimeFieldNames);
    }

}
