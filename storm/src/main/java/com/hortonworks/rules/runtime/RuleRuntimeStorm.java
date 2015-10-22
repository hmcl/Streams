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
import com.hortonworks.iotas.layout.design.rule.Rule;
import com.hortonworks.iotas.layout.design.rule.condition.script.Script;
import com.hortonworks.iotas.layout.design.rule.exception.ConditionEvaluationException;
import com.hortonworks.iotas.layout.runtime.processor.ProcessorRuntime;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntime;

import javax.script.ScriptException;
import java.util.Arrays;

public class RuleRuntimeStorm implements RuleRuntime<Tuple, IOutputCollector> {
    private final ProcessorRuntime<OutputFieldsDeclarer> processorRuntime;
    private final Rule<Schema.Field> rule;
    private final Script<Tuple, Schema.Field> script;     // Script used to evaluate the condition

    public RuleRuntimeStorm(ProcessorRuntime<OutputFieldsDeclarer> processorRuntime,
                            Rule<Schema.Field> rule, Script<Tuple, Schema.Field> script) {
        this.processorRuntime = processorRuntime;
        this.rule = rule;
        this.script = script;
    }

    @Override
    public boolean evaluate(Tuple input) {
        logger.debug("Evaluating condition for rule: [{}] \n\tinput tuple: [{}]", rule, input);
        try {
            return script.evaluate(input);
        } catch (ScriptException e) {
            throw new ConditionEvaluationException("Exception occurred when evaluating condition. " + this, e);
        }
    }

    @Override
    public void execute(Tuple input, IOutputCollector collector) {
        logger.debug("Executing rule: [{}] \n\t input tuple: [{}] \n\t collector: [{}]", rule, input, collector);
        collector.emit(((RulesProcessorRuntimeStorm)processorRuntime).getStreamId(rule), Arrays.asList(input), input.getValues());
    }

    @Override
    public String toString() {
        return "RuleRuntimeStorm{" +
                "processorRuntime=" + processorRuntime +
                ", rule=" + rule +
                ", script=" + script +
                '}';
    }
}
