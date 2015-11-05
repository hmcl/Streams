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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.hortonworks.iotas.common.IotasEvent;
import com.hortonworks.iotas.layout.design.rule.Rule;
import com.hortonworks.iotas.layout.design.rule.exception.ConditionEvaluationException;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntime;
import com.hortonworks.iotas.layout.runtime.rule.condition.script.Script;

import javax.script.ScriptException;
import java.util.Arrays;

public class RuleRuntimeStorm implements RuleRuntime<Tuple, IOutputCollector, OutputFieldsDeclarer> {
    private final Rule rule;
    private final Script<Tuple, ?, ?> script;     // Script used to evaluate the condition

    public RuleRuntimeStorm(Rule rule, Script<Tuple, ?, ?> script) {
        this.rule = rule;
        this.script = script;
    }

    @Override
    public boolean evaluate(Tuple input) {
        try {
            Object valueByField = input.getValueByField(IotasEvent.IOTAS_EVENT);
            log.debug("valueByField = " + valueByField);
            log.debug("Evaluating condition for Rule: [{}] \n\tInput tuple: [{}]", rule, input);
            final boolean evaluates = script.evaluate(input);
           log.debug("Rule condition evaluated to: [{}]. Rule: [{}] \n\tInput tuple: [{}]", evaluates, rule, input);
            return evaluates;
        } catch (ScriptException e) {
            throw new ConditionEvaluationException("Exception occurred when evaluating rule condition. " + this, e);
        }
    }

    @Override
    public void execute(Tuple input, IOutputCollector collector) {
        log.debug("Executing rule: [{}] \n\tInput tuple: [{}] \n\tCollector: [{}] \n\tStream:[{}]",
                  rule, input, collector, getStreamId());
        collector.emit(getStreamId(), Arrays.asList(input), input.getValues());
    }

    public void declareOutput(OutputFieldsDeclarer declarer) {
        declarer.declareStream(getStreamId(), getFields());
    }

    //TODO
    public String getStreamId() {
        return rule.getRuleProcessorName() + "." + rule.getName() + "." + rule.getId();
    }

    private Fields getFields() {
        return new Fields(IotasEvent.IOTAS_EVENT);
    }

    @Override
    public String toString() {
        return "RuleRuntimeStorm{" +
                "rule=" + rule +
                ", script=" + script +
                '}';
    }
}
