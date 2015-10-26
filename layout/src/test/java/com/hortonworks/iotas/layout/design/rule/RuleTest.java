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
import com.hortonworks.iotas.common.Schema.Field;
import com.hortonworks.iotas.layout.design.processor.Processor;
import com.hortonworks.iotas.layout.design.processor.RulesProcessor;
import com.hortonworks.iotas.layout.design.processor.Sink;
import com.hortonworks.iotas.layout.design.rule.action.Action;
import com.hortonworks.iotas.layout.design.rule.condition.Condition;
import com.hortonworks.iotas.layout.design.rule.condition.Condition.ConditionElement;
import com.hortonworks.iotas.layout.design.rule.condition.Condition.ConditionElement.LogicalOperator;
import com.hortonworks.iotas.layout.design.rule.condition.Condition.FieldConditionElement;
import com.hortonworks.iotas.layout.design.rule.condition.expression.GroovyExpressionBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class RuleTest {

    @Test
    public void testBuildRuleProcessor() throws Exception {
        final RulesProcessor<Schema, Schema, Field> rulesProcessor = new RuleProcessorBuilder(1, 2, 2).build();

        //JSON
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(Feature.FAIL_ON_EMPTY_BEANS, false);
        String ruleProcessorJson = mapper.writeValueAsString(rulesProcessor);
        System.out.println(ruleProcessorJson);
        //TODO
//        mapper.registerSubtypes(FieldConditionElement.class);
//        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
//        RulesProcessor<Schema, Schema, Field> rulesProcessor1 = mapper.readValue(ruleProcessorJson, RulesProcessor.class);
//        System.out.println(rulesProcessor);
//        System.out.println(rulesProcessor1);
    }

    private static class RuleProcessorBuilder {
        public static final String TEMPERATURE = "temperature";
        public static final String HUMIDITY = "humidity";

        private final long id;
        private final int numRules;
        private final int numSinks;
        private Schema declaredInputsOutputs;

        public RuleProcessorBuilder(long id, int numRules, int numSinks) {
            this.id = id;
            this.numRules = numRules;
            this.numSinks = numSinks;
        }

        public RulesProcessor<Schema, Schema, Field> build() {
            RulesProcessor<Schema, Schema, Field> rulesProcessor = new RulesProcessor<>();
            rulesProcessor.setDeclaredInput(buildDeclaredInputsOutputs());
            rulesProcessor.setId(id);
            rulesProcessor.setName("rule_processsor_" + id);
            rulesProcessor.setDescription("rule_processsor_" + id + "_desc");
            rulesProcessor.setRules(buildRules());
            return rulesProcessor;
        }

        private Schema buildDeclaredInputsOutputs() {
            final Schema declaredInputsOutputs = new Schema.SchemaBuilder().fields(new ArrayList<Field>() {{
                add(new Field(TEMPERATURE, Schema.Type.INTEGER));
                add(new Field(HUMIDITY, Schema.Type.INTEGER));
            }}).build();
            this.declaredInputsOutputs = declaredInputsOutputs;
            return declaredInputsOutputs;
        }

        private List<Rule<Schema, Field>> buildRules() {
            List<Rule<Schema, Field>> rules = new ArrayList<>();
            for (int i = 1; i <= numRules; i++) {
                rules.add(buildRule(i, buildCondition(), buildAction(buildSinks())));
            }
            return rules;
        }

        private Rule<Schema, Field> buildRule(long ruleId, Condition<Field> condition, Action<Schema> action) {
            Rule<Schema, Field> rule = new Rule<>(condition, action);
            rule.setId(ruleId);
            rule.setName("rule_" + ruleId);
            rule.setDescription("rule_" + ruleId + "_desc");
            rule.setRuleProcessorName("rule_processsor_" + id);
            return rule;
        }

        private Action<Schema> buildAction(List<Processor<Schema>> sinks) {
            Action<Schema> action = new Action<>();
            action.setDeclaredOutput(declaredInputsOutputs);
            action.setProcessors(sinks);
            return action;
        }

        private List<Processor<Schema>> buildSinks() {
            List<Processor<Schema>> sinks = new ArrayList<>();
            for (int i = 1; i <= numSinks; i++) {
                sinks.add(buildSink(i));
            }
            return sinks;
        }

        private Processor<Schema> buildSink(long sinkId) {
            Processor<Schema> sink = new Sink<>();
            sink.setId(id);
            sink.setName("sink_" + sinkId);
            sink.setDescription("sink_" + sinkId + "_desc");
            sink.setDeclaredInput(declaredInputsOutputs);
            return sink;
        }

        private Condition<Field> buildCondition() {
            return buildCondition(buildConditionElements());
        }

        private Condition<Field> buildCondition(List<ConditionElement<Field>> conditionElements) {
            Condition<Field> condition = new Condition<>();
            condition.setConditionElements(conditionElements);
            return condition;
        }

        private List<ConditionElement<Field>> buildConditionElements() {
            List<ConditionElement<Field>> conditionElements = new ArrayList<>();
            conditionElements.add(buildConditionElement(TEMPERATURE, "100", LogicalOperator.OR));
            conditionElements.add(buildConditionElement(HUMIDITY, "50", null));
            return conditionElements;
        }

        private ConditionElement<Field> buildConditionElement(
                String firstOperand, String secondOperand, LogicalOperator logicalOperator) {
            ConditionElement<Field> conditionElement =
                    new FieldConditionElement(new GroovyExpressionBuilder());
            final Field temperature = new Field(firstOperand, Schema.Type.INTEGER);
            conditionElement.setFirstOperand(temperature);
            conditionElement.setOperation(ConditionElement.Operation.GREATER_THAN);
            conditionElement.setSecondOperand(secondOperand);
            if (logicalOperator != null) {
                conditionElement.setLogicalOperator(logicalOperator);
            }
            return conditionElement;
        }
    }
}