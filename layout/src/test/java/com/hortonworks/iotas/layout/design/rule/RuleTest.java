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
import com.hortonworks.iotas.layout.design.processor.RulesProcessor;
import com.hortonworks.iotas.layout.design.rule.action.Action;
import com.hortonworks.iotas.layout.design.rule.condition.Condition;
import com.hortonworks.iotas.layout.design.rule.condition.Condition.ConditionElement;
import com.hortonworks.iotas.layout.design.rule.condition.Condition.ConditionElement.LogicalOperator;
import com.hortonworks.iotas.layout.design.rule.condition.expression.GroovyExpressionBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class RuleTest {
    @Test
    public void testBuildRule() throws Exception {
        // Condition
        final Condition<Schema.Field> condition = createCondition();

        // Action
        Processor processor = new RulesProcessor();
        processor.setId(1L);
        processor.setName("rule_processor_1");
        processor.setDescription("rule_processor_1_desc");

        List<Processor> processors = new LinkedList<>();
        processors.add(processor);

        List<Schema.Field> declaredOutputs = new ArrayList<Schema.Field>() {{
            add(condition.getConditionElements().get(0).getFirstOperand());
            add(condition.getConditionElements().get(1).getFirstOperand());
        }};

        Action<Schema.Field> action = new Action<>();
        action.setDeclaredOutput(declaredOutputs);
        action.setProcessors(processors);

        Rule<Schema.Field> rule = new Rule<>(condition, action);
        rule.setId(2L);
        rule.setName("rule_1");
        rule.setDescription("rule_1_desc");


        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(Feature.FAIL_ON_EMPTY_BEANS, false);
        String ruleJson = mapper.writeValueAsString(rule);
        System.out.println(ruleJson);
    }

    private Condition<Schema.Field> createCondition() {
        return createCondition(createConditionElements());
    }

    private Condition<Schema.Field> createCondition(List<ConditionElement<Schema.Field>> conditionElements) {
        Condition<Schema.Field> condition = new Condition();
        condition.setConditionElements(conditionElements);
        return condition;
    }

    private List<ConditionElement<Schema.Field>> createConditionElements() {
        List<ConditionElement<Schema.Field>> conditionElements = new ArrayList<>();
        conditionElements.add(createConditionElement("temperature", "100", LogicalOperator.OR));
        conditionElements.add(createConditionElement("humidity", "50", null));
        return conditionElements;
    }

    private ConditionElement<Schema.Field> createConditionElement(
            String firstOperand, String secondOperand, LogicalOperator logicalOperator) {
        ConditionElement<Schema.Field> conditionElement =
                new ConditionElement<>(new GroovyExpressionBuilder());
        final Schema.Field temperature = new Schema.Field(firstOperand, Schema.Type.INTEGER);
        conditionElement.setFirstOperand(temperature);
        conditionElement.setOperation(ConditionElement.Operation.GREATER_THAN);
        conditionElement.setSecondOperand(secondOperand);
        conditionElement.setLogicalOperator(logicalOperator);
        return conditionElement;
    }


}