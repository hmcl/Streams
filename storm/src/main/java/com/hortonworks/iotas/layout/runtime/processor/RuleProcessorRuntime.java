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

import backtype.storm.topology.OutputFieldsDeclarer;
import com.hortonworks.iotas.layout.design.component.RulesProcessor;
import com.hortonworks.iotas.layout.design.rule.Rule;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntimeBuilder;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntimeStorm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * Object representing a design time rules processor.
 */
public class RuleProcessorRuntime implements Serializable {
    protected static final Logger log = LoggerFactory.getLogger(RuleProcessorRuntime.class);

    protected RulesProcessor rulesProcessor;
    protected List<RuleRuntimeStorm> rulesRuntime;

    private RuleProcessorRuntime(Builder builder) {
        this.rulesProcessor = builder.rulesProcessor;
        this.rulesRuntime = builder.rulesRuntime;
    }

    public static class Builder {
        private final RulesProcessor rulesProcessor;
        private final RuleRuntimeBuilder ruleRuntimeBuilder;
        private List<RuleRuntimeStorm> rulesRuntime;

        public Builder(RulesProcessor rulesProcessor, RuleRuntimeBuilder ruleRuntimeBuilder) {
            this.rulesProcessor = rulesProcessor;
            this.ruleRuntimeBuilder = ruleRuntimeBuilder;
        }

        public RuleProcessorRuntime build() {
            final List<Rule> rules = rulesProcessor.getRules();
            rulesRuntime = new ArrayList<>();

            if (rules != null) {
                for (Rule rule : rules) {
                    ruleRuntimeBuilder.buildExpression(rule);
                    ruleRuntimeBuilder.buildScriptEngine();
                    ruleRuntimeBuilder.buildScript();
                    final RuleRuntimeStorm ruleRuntime = ruleRuntimeBuilder.getRuleRuntime(rule);
                    rulesRuntime.add(ruleRuntime);
                    log.trace("Added {}", ruleRuntime);
                }
                log.debug("Finished building: {}", this);
            }
            return new RuleProcessorRuntime(this);
        }
    }

    public List<RuleRuntimeStorm> getRulesRuntime() {
        return rulesRuntime;
    }

    public void declareOutput(OutputFieldsDeclarer declarer) {
        for (RuleRuntimeStorm ruleRuntime:rulesRuntime) {
            ruleRuntime.declareOutput(declarer);
        }
    }

    public void setRulesRuntime(List<RuleRuntimeStorm> rulesRuntime) {
        this.rulesRuntime = rulesRuntime;
    }

    public RulesProcessor getRuleProcessor() {
        return rulesProcessor;
    }

    public void setRuleProcessor(RulesProcessor rulesProcessor) {
        this.rulesProcessor = rulesProcessor;
    }

    @Override
    public String toString() {
        return "RuleProcessorRuntime{" +
                "rulesProcessor=" + rulesProcessor +
                ", rulesRuntime=" + rulesRuntime +
                '}';
    }
}

