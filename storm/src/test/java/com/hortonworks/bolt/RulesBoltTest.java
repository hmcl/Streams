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

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.hortonworks.bolt.rules.RulesBolt;
import com.hortonworks.iotas.common.IotasEvent;
import com.hortonworks.iotas.common.IotasEventImpl;
import com.hortonworks.iotas.layout.runtime.processor.RuleProcessorRuntimeStorm;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntimeStorm;
import com.hortonworks.iotas.layout.runtime.rule.topology.RuleProcessorMockBuilder;
import com.hortonworks.iotas.layout.runtime.rule.topology.RulesTopologyTest;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import mockit.VerificationsInOrder;
import mockit.integration.junit4.JMockit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

@RunWith(JMockit.class)
public class RulesBoltTest extends RulesTopologyTest {
    protected static final Logger log = LoggerFactory.getLogger(RulesBoltTest.class);

    // JUnit constructs for printing which tests are being run
    public @Rule TestName testName = new TestName();
    public @Rule TestWatcher watchman = new TestWatcher() {
        @Override
        public void starting(final Description method) {
            log.debug("RUNNING TEST [{}] ", method.getMethodName());
        }
    };

    private static final IotasEventImpl IOTAS_EVENT = new IotasEventImpl(new HashMap<String, Object>() {{
        put(RuleProcessorMockBuilder.TEMPERATURE, 101);
        put(RuleProcessorMockBuilder.HUMIDITY, 51);
    }}, "dataSrcId_1", "23");

    private static final Values IOTAS_EVENT_VALUES = new Values(IOTAS_EVENT);

    private static final IotasEventImpl IOTAS_EVENT_INVALID_FIELDS = new IotasEventImpl(new HashMap<String, Object>() {{
        put("non_existent_field1", 101);
        put("non_existent_field2", 51);
        put("non_existent_field3", 23);
    }}, "dataSrcId_2", "24");

    private static final Values IOTAS_EVENT_INVALID_FIELDS_VALUES = new Values(IOTAS_EVENT_INVALID_FIELDS);

    private @Tested RulesBolt rulesBolt;
    private @Injectable OutputCollector mockOutputCollector;
    private @Injectable Tuple mockTuple;
    private RuleProcessorRuntimeStorm ruleProcessorRuntime;

    @Before
    public void setup() throws Exception {
        ruleProcessorRuntime = createRulesProcessorRuntime();
        rulesBolt = (RulesBolt) createRulesBolt(ruleProcessorRuntime);
    }

    @Test
    public void test_validTuple_oneRuleEvaluates_acks() throws Exception {
        new Expectations() {{
            mockTuple.getValues(); result = IOTAS_EVENT_VALUES;
            mockTuple.getValueByField(IotasEvent.IOTAS_EVENT); returns(IOTAS_EVENT);
        }};

        executeAndVerifyCollectorCalls(true, 1);
    }

    @Test
    public void test_invalidTuple_ruleDoesNotEvaluate_acks() throws Exception {
        new Expectations() {{
            mockTuple.getValueByField(IotasEvent.IOTAS_EVENT); returns(null);
        }};

        executeAndVerifyCollectorCalls(true, 0);
    }

    @Test
    public void test_tupleInvalidFields_ruleDoesNotEvaluate_fails() throws Exception {
//        test_validTuple_oneRuleEvaluates_acks();
        new Expectations() {{
            mockTuple.getValueByField(IotasEvent.IOTAS_EVENT); returns(IOTAS_EVENT_INVALID_FIELDS);
        }};

        executeAndVerifyCollectorCalls(false, -1);
    }

    private void executeAndVerifyCollectorCalls(final boolean isSuccess, final int rule2NumTimes) {
        rulesBolt.execute(mockTuple);

        if(isSuccess) {
            new VerificationsInOrder() {{
                mockOutputCollector.emit(((RuleRuntimeStorm)ruleProcessorRuntime.getRulesRuntime().get(0)).getStreamId(),
                        mockTuple, IOTAS_EVENT_VALUES); times = 0;  // rule 1 does not trigger

                Values actualValues;
                mockOutputCollector.emit(((RuleRuntimeStorm)ruleProcessorRuntime.getRulesRuntime().get(1)).getStreamId(),
                        mockTuple, actualValues = withCapture()); times = rule2NumTimes;    // rule 2 triggers rule2NumTimes

                Assert.assertEquals(IOTAS_EVENT_VALUES, actualValues);
                Assert.assertNotEquals(IOTAS_EVENT_VALUES, IOTAS_EVENT_INVALID_FIELDS_VALUES);
                Assert.assertNotEquals(IOTAS_EVENT_INVALID_FIELDS_VALUES, actualValues);
                mockOutputCollector.ack(mockTuple); times = 1;
            }};
        } else {
            new VerificationsInOrder() {{
                mockOutputCollector.fail(mockTuple);
            }};
        }
    }
}
