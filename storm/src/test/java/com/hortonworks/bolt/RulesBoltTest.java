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
import com.hortonworks.iotas.common.IotasEvent;
import com.hortonworks.iotas.common.IotasEventImpl;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.component.RulesProcessor;
import com.hortonworks.iotas.layout.runtime.processor.RuleProcessorRuntimeBuilder;
import com.hortonworks.iotas.layout.runtime.processor.RuleProcessorRuntimeConstructor;
import com.hortonworks.iotas.layout.runtime.processor.RulesProcessorRuntimeStormBuilder;
import com.hortonworks.iotas.layout.runtime.rule.GroovyRuleRuntimeBuilder;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntimeBuilder;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntimeStorm;
import com.hortonworks.iotas.layout.runtime.rule.condition.expression.SchemaFieldNameTypeExtractor;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import mockit.VerificationsInOrder;
import mockit.integration.junit4.JMockit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

@RunWith(JMockit.class)
public class RulesBoltTest {
    protected static final Logger log = LoggerFactory.getLogger(RulesBoltTest.class);

    public static final IotasEventImpl IOTAS_EVENT = new IotasEventImpl(new HashMap<String, Object>() {{
        put("temperature", 99);
        put("humidity", 51);
    }}, "dataSrcId", "23");

    private static final Values VALUES = new Values(IOTAS_EVENT);

    //TODO: Check all of this

    private RulesProcessor<Schema, Schema, Schema.Field> rulesProcessor;
    private @Tested RulesBolt<Schema, Schema, Schema.Field> rulesBolt;

    private @Injectable OutputCollector mockOutputCollector;
    private @Injectable Tuple mockTuple;

    @Before
    public void setup() throws Exception {
        rulesProcessor = new RuleProcessorMockBuilder(1,2,2).build();
        RuleRuntimeBuilder ruleRuntimeBuilder = new GroovyRuleRuntimeBuilder(new SchemaFieldNameTypeExtractor());
        RuleProcessorRuntimeBuilder runtimeBuilder = new RulesProcessorRuntimeStormBuilder(rulesProcessor, ruleRuntimeBuilder);
        RuleProcessorRuntimeConstructor runtimeConstructor = new RuleProcessorRuntimeConstructor(runtimeBuilder);
        runtimeConstructor.construct();
        rulesBolt = new RulesBolt<>(runtimeConstructor.getRuleProcessorRuntime());

        rulesBolt.prepare(null, null, mockOutputCollector);
    }

    @Test
    public void testRulesTriggers() throws Exception {
        new Expectations() {{
            mockTuple.getValueByField(IotasEvent.IOTAS_EVENT); returns(IOTAS_EVENT);
//            times = 2;

//            rulesProcessorRuntimeStorm.getRulesRuntime().get(0).evaluate(mockTuple); result = true;
        }};

        callExecuteAndVerifyCollectorInteraction(true);

//        assertTrue(rulesBolt.getRulesRuntime().get(0).evaluate(mockTuple));   //TODO: Check
    }

    private void callExecuteAndVerifyCollectorInteraction(final boolean isSuccess) {
        log.debug("callExecuteAndVerifyCollectorInteraction");
        rulesBolt.execute(mockTuple);

        if(isSuccess) {
            new VerificationsInOrder() {{
//                rulesBolt.getRulesRuntime().get(0).evaluate(mockTuple); times = 1;    //TODO: Check
                mockOutputCollector.emit(((RuleRuntimeStorm)rulesBolt.getRulesRuntime().get(0)).getStreamId(), mockTuple, withAny(VALUES)); times = 1;
//                mockOutputCollector.emit(((RuleRuntimeStorm)rulesBolt.getRulesRuntime().get(1)).getStreamId(), mockTuple, withAny(VALUES)); times = 1;
                mockOutputCollector.ack(mockTuple); times = 1;
            }};

        } else {
            new VerificationsInOrder() {{
                mockOutputCollector.fail(mockTuple);
            }};
        }
    }

}
