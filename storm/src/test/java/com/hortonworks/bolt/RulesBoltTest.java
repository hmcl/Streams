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

package com.hortonworks.bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.processor.RulesProcessor;
import com.hortonworks.rules.runtime.RulesProcessorRuntimeStorm;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Tested;
import mockit.VerificationsInOrder;
import mockit.integration.junit4.JMockit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JMockit.class)
public class RulesBoltTest {
    private static final Values VALUES = new Values("temperature","humidity");

    private RulesProcessor<Schema, Schema, Schema.Field> rulesProcessorMock;
    private @Tested RulesBolt rulesBolt;

    private @Mocked OutputCollector mockOutputCollector;
    private @Mocked Tuple mockTuple;
    private RulesProcessorRuntimeStorm rulesProcessorRuntimeStorm;

    @Before
    public void setup() throws Exception {
        rulesProcessorMock = new RuleProcessorMockBuilder(1,2,2).build();
        rulesProcessorRuntimeStorm = new RulesProcessorRuntimeStorm(rulesProcessorMock);

        rulesBolt = new RulesBolt(rulesProcessorRuntimeStorm);

        Config config = new Config();
        rulesBolt.prepare(config, null, mockOutputCollector);
    }

    @Test
    public void testRulesTriggers() throws Exception {
        new Expectations() {{
            mockTuple.getBinaryByField(RuleProcessorMockBuilder.TEMPERATURE); returns(51);
            rulesProcessorRuntimeStorm.getRulesRuntime().get(0).evaluate(mockTuple); result = true;
        }};

        callExecuteAndVerifyCollectorInteraction(true);
    }

    private void callExecuteAndVerifyCollectorInteraction(boolean isSuccess) {
        rulesBolt.execute(mockTuple);

        if(isSuccess) {
            new VerificationsInOrder() {{
                mockOutputCollector.emit(mockTuple, withAny(VALUES));
                mockOutputCollector.ack(mockTuple); times = 1;
            }};

        } else {
            new VerificationsInOrder() {{
                mockOutputCollector.fail(mockTuple);
            }};
        }
    }

}
