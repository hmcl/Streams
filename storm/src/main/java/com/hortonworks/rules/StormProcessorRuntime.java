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
import backtype.storm.tuple.Tuple;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.rule.Rule;
import com.hortonworks.iotas.layout.rule.condition.script.Script;
import com.hortonworks.iotas.layout.rule.exception.ConditionEvaluationException;
import com.hortonworks.iotas.layout.rule.runtime.RuleRuntime;

import javax.script.ScriptException;
import java.util.Arrays;
import java.util.List;

public class StormProcessorRuntime {
    private List<RuleRuntime<Tuple, IOutputCollector>> rules;





    public StormProcessorRuntime(Rule<Schema.Field> rule, Script<Tuple, Schema.Field> script) {
        this.rule = rule;
        this.script = script;
        compileScript();            //TODO - this can probably be static
    }

    private void compileScript() {
        script.compile(rule.getCondition());
    }

    @Override
    public boolean evaluate(Tuple input) {
        logger.debug("Evaluating condition for rule: [{}] \n\tinput tuple: [{}]", rule, input);
        try {
            return script.evaluate(input);
        } catch (ScriptException e) {
            throw new ConditionEvaluationException("Exception occurred when evaluating condition: " + this, e);
        }
    }

    @Override
    public void execute(Tuple input, IOutputCollector collector) {
        logger.debug("Executing rule: [{}] \n\t input tuple: [{}] \n\t collector: [{}]", rule, input, collector);
        collector.emit(getStreamId(), Arrays.asList(input), input.getValues());
    }

    @Override
    public String toString() {
        return "StormRuleRuntime{" +
                "rule=" + rule +
                ", script=" + script +
                '}';
    }

    public String getStreamId() {
        return rule.get;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(getStreamId(), getFields());
    }

}
