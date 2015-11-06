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
import com.hortonworks.iotas.layout.design.component.RulesProcessor;
import com.hortonworks.iotas.layout.runtime.processor.RuleProcessorRuntime;
import com.hortonworks.iotas.layout.runtime.processor.RuleProcessorRuntimeBuilder;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntime;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntimeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RuleProcessorRuntimeStorm implements RuleProcessorRuntime<Tuple, IOutputCollector, OutputFieldsDeclarer> {
    public static final Logger log = LoggerFactory.getLogger(RuleProcessorRuntimeStorm.class);  //TODO

    protected RulesProcessor rulesProcessor;
    protected List<RuleRuntimeStorm> rulesRuntime;

    public static class Builder extends RuleProcessorRuntimeBuilder {
        public Builder(RulesProcessor rulesProcessor, RuleRuntimeBuilder ruleRuntimeBuilder) {
            super(rulesProcessor, ruleRuntimeBuilder);
        }



        @Override
        public RuleProcessorRuntime getRuleProcessorRuntime() {
            return new RuleProcessorRuntimeStorm(rulesProcessor, rulesRuntime) ;
        }
    }

    /*public RuleProcessorRuntimeStorm(RulesRuntimeStormBuilder<Tuple, IOutputCollector> rulesRuntimeBuilder) {
        rulesRuntime = rulesRuntimeBuilder.getRulesRuntime();
    }*/

    public RuleProcessorRuntimeStorm(List<RuleRuntimeStorm> rulesRuntime) {
        this.rulesRuntime = rulesRuntime;
    }

    public List<RuleRuntimeStorm> getRulesRuntime() {
        return rulesRuntime;
    }

    public void declareOutput(OutputFieldsDeclarer declarer) {
        for (RuleRuntimeStorm ruleRuntime:rulesRuntime) {
            ruleRuntime.declareOutput(declarer);
        }
    }

    @Override
    public void setRulesRuntime(List<? extends RuleRuntime<Tuple, IOutputCollector, OutputFieldsDeclarer>> rulesRuntime) {
        this.rulesRuntime = (List<RuleRuntimeStorm>) rulesRuntime;      //TODO: Try avoiding this cast
    }

    @Override
    public RulesProcessor getRuleProcessor() {
        return rulesProcessor;
    }

    @Override
    public void setRuleProcessor(RulesProcessor rulesProcessor) {
        this.rulesProcessor = rulesProcessor;
    }
}

