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

package com.hortonworks.iotas.rules.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.hortonworks.iotas.rules.Rule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RulesBolt extends BaseRichBolt {
    private List<Rule> rules;
    private OutputCollector collector;

    public RulesBolt(List<Rule> rules) {
        this.rules = rules;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        for (Rule rule : rules) {
            Map<String, Object> stringObjectMap = buildMap(input);
            if (rule.evaluate(stringObjectMap)) {
                rule.execute(stringObjectMap);
            }
        }
        collector.ack(input);   //TODO ack all or nothing?
    }

    private Map<String, Object> buildMap(Tuple input) {
        return new HashMap<>();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
