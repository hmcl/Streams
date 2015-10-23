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

package com.hortonworks.iotas.layout.design.rule;

import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.processor.Processor;
import com.hortonworks.iotas.layout.design.processor.RulesProcessorImpl;
import com.hortonworks.iotas.layout.design.rule.action.Action;
import com.hortonworks.iotas.layout.design.rule.action.ActionImpl;
import com.hortonworks.iotas.layout.design.rule.condition.Condition;
import com.hortonworks.iotas.layout.design.rule.condition.ConditionImpl;
import com.hortonworks.iotas.layout.design.rule.condition.expression.GroovyExpressionBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class RuleImplTest {
    @Test
    public void testBuildRule() throws Exception {

        // Condition
        Condition.ConditionElement<Schema.Field> conditionElement1 = new ConditionImpl.ConditionElementImpl(new GroovyExpressionBuilder());
        final Schema.Field temperature = new Schema.Field("temperature", Schema.Type.INTEGER);
        conditionElement1.setFirstOperand(temperature);
        conditionElement1.setOperation(Condition.ConditionElement.Operation.GREATER_THAN);
        conditionElement1.setSecondOperand("100");
        conditionElement1.setLogicalOperator(Condition.ConditionElement.LogicalOperator.OR);

        Condition.ConditionElement<Schema.Field> conditionElement2 = new ConditionImpl.ConditionElementImpl(new GroovyExpressionBuilder());
        final Schema.Field humidity = new Schema.Field("humidity", Schema.Type.INTEGER);
        conditionElement2.setFirstOperand(humidity);
        conditionElement2.setOperation(Condition.ConditionElement.Operation.LESS_THAN);
        conditionElement2.setSecondOperand("50");

        List<Condition.ConditionElement<Schema.Field>> conditionElements = new ArrayList<>();
        conditionElements.add(conditionElement1);
        conditionElements.add(conditionElement2);

        Condition<Schema.Field> condition = new ConditionImpl();
        condition.setConditionElements(conditionElements);
        
        // Action
        Processor processor = new RulesProcessorImpl();
        processor.setId(1L);
        processor.setName("rule_processor_1");
        processor.setDescription("rule_processor_1_desc");

        List<Processor> processors = new LinkedList<>();
        processors.add(processor);

        List<Schema.Field> declaredOutputs = new ArrayList<Schema.Field>(){{add(temperature); add(humidity);}};

        Action<Schema.Field> action = new ActionImpl();
        action.setDeclaredOutput(declaredOutputs);
        action.setProcessors(processors);

        Rule<Schema.Field> rule = new RuleImpl(condition, action);
        rule.setId(2L);
        rule.setName("rule_1");
        rule.setDescription("rule_1_desc");



        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(org.codehaus.jackson.map.SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
        String ruleJson = mapper.writeValueAsString(rule);
        System.out.println(ruleJson);
    }

    String json = "processor { id: \"\", name: \"\", }";

}