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

package com.hortonworks.rules.topology;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import com.hortonworks.bolt.RulesBolt;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.component.RulesProcessor;
import com.hortonworks.iotas.layout.design.rule.condition.expression.SchemaFieldNameTypeExtractor;
import com.hortonworks.rules.runtime.GroovyRuleRuntimeBuilder;

public class RulesTopologyTest {

    private static final String RULES_TEST_SPOUT = "RulesTestSpout";
    private static final String RULES_BOLT = "rulesBolt";
    private static final String RULES_TEST_SINK_BOLT = "RulesTestSinkBolt";
    private static final String RULES_TEST_SINK_BOLT_1 = RULES_TEST_SINK_BOLT + "_1";
    private static final String RULES_TEST_SINK_BOLT_2 = RULES_TEST_SINK_BOLT + "_2";

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(RULES_TEST_SPOUT, new RulesTestSpout());
        builder.setBolt(RULES_BOLT, getRulesBolt()).shuffleGrouping(RULES_TEST_SPOUT);
        builder.setBolt(RULES_TEST_SINK_BOLT_1, new RulesTestSinkBolt()).shuffleGrouping(RULES_BOLT, getStream1());
        builder.setBolt(RULES_TEST_SINK_BOLT_2, new RulesTestSinkBolt()).shuffleGrouping(RULES_BOLT, getStream2());


        final Config config = new Config();
        config.setDebug(true);
//        conf.setNumWorkers(3);
//        conf.setMaxTaskParallelism(3);

        ILocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("RulesTopologyTest", config, builder.createTopology());
    }

    public static IRichBolt getRulesBolt() {
        RulesProcessor<Schema, Schema, Schema.Field> rulesProcessorMock = new RuleProcessorMockBuilder(1,2,2).build();
        RulesBolt<Schema, Schema, Schema.Field> rulesBolt = new RulesBolt<>(rulesProcessorMock,
                new GroovyRuleRuntimeBuilder<Schema, Schema.Field>(new SchemaFieldNameTypeExtractor()));

        return rulesBolt;
    }

    public static String getStream1() {
        return "rule_processsor_1.rule_1.1";
    }


    private static String getStream2() {
        return "rule_processsor_1.rule_2.2";
    }
}
