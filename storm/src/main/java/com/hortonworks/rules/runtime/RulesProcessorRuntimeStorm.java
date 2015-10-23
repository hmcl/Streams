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

import backtype.storm.topology.OutputFieldsDeclarer;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.processor.RulesProcessor;
import com.hortonworks.iotas.layout.design.rule.Rule;
import com.hortonworks.iotas.layout.runtime.processor.ProcessorRuntime;
import com.hortonworks.rules.condition.script.GroovyScript;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RulesProcessorRuntimeStorm implements ProcessorRuntime<OutputFieldsDeclarer> {
    public static final Logger logger = LoggerFactory.getLogger(RulesProcessorRuntimeStorm.class);  //TODO

    private RulesProcessor<Schema, Schema, Schema.Field> rulesProcessor;
    private List<RuleRuntimeStorm> rulesRuntime;

    /*public RulesProcessorRuntimeStorm(RulesRuntimeBuilder<Tuple, IOutputCollector> rulesRuntimeBuilder) {
        rulesRuntime = rulesRuntimeBuilder.getRulesRuntime();
    }*/

    public RulesProcessorRuntimeStorm(RulesProcessor<Schema, Schema, Schema.Field> processor) {
        this.rulesProcessor = processor;
        buildAndSetRulesRuntime();             //TODO: Inject this to make it easier to test
    }

    private void buildAndSetRulesRuntime() {
        final List<Rule<Schema.Field>> rules = rulesProcessor.getRules();
        rulesRuntime = new ArrayList<>(rules.size());
        for (Rule<Schema.Field> rule : rules) {
            rulesRuntime.add(new RuleRuntimeStorm(this, rule, new GroovyScript(rule.getCondition())));      // TODO: Make scripting language pluggable
        }
    }

    public List<RuleRuntimeStorm> getRulesRuntime() {
        return rulesRuntime;
    }

    public void declareOutput(OutputFieldsDeclarer declarer) {
        for (RuleRuntimeStorm ruleRuntime:rulesRuntime) {
            ruleRuntime.declareOutput(rulesProcessor.getName(), declarer);
        }
    }

    interface StormOutputBuilder {
        String getStreamId();
    }

    interface constructor {
        Building construct(Builder builder);
    }

    interface Builder {
        void buildPart();
        Building getBuilding();

    }

    class HouseBuilder implements Builder {

        @Override
        public void buildPart() {

        }

        @Override
        public Building getBuilding() {
            return null;
        }
    }

    interface Building {
        int getNumRooms();
    }

    interface Office extends Building {

    }

    interface House extends Building {

    }

}

