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
import com.hortonworks.iotas.layout.design.component.Component;
import com.hortonworks.iotas.layout.design.component.RulesProcessor;
import com.hortonworks.iotas.layout.design.rule.action.Action;
import com.hortonworks.iotas.layout.design.rule.condition.Condition;

import java.util.ArrayList;
import java.util.List;

public class RuleProcessorMockBuilder {
    public static final String TEMPERATURE = "temperature";
    public static final String HUMIDITY = "humidity";

    private final long ruleProcessorId;
    private final int numRules;
    private final int numSinks;
    private List declaredInputsOutputs;

    public RuleProcessorMockBuilder(long ruleProcessorId, int numRules, int numSinksPerRule) {
        this.ruleProcessorId = ruleProcessorId;
        this.numRules = numRules;
        this.numSinks = numSinksPerRule;
    }

    public RulesProcessor build() {
        RulesProcessor rulesProcessor = new RulesProcessor();
        rulesProcessor.setDeclaredInput(buildDeclaredInputsOutputs());
        rulesProcessor.setId(ruleProcessorId);
        rulesProcessor.setName("rule_processsor_" + ruleProcessorId);
        rulesProcessor.setDescription("rule_processsor_" + ruleProcessorId + "_desc");
        rulesProcessor.setRules(buildRules());
        return rulesProcessor;
    }

    private List buildDeclaredInputsOutputs() {
        final Schema declaredInputsOutputs = new Schema.SchemaBuilder().fields(new ArrayList() {{
            add(new Schema.Field(TEMPERATURE, Schema.Type.INTEGER));
            add(new Schema.Field(HUMIDITY, Schema.Type.INTEGER));
        }}).build();
        ;
        this.declaredInputsOutputs = declaredInputsOutputs.getFields();
        return declaredInputsOutputs.getFields();
    }

    private List<Rule> buildRules() {
        List<Rule> rules = new ArrayList<>();
        for (int i = 1; i <= numRules; i++) {
            rules.add(buildRule(i, buildCondition(i), buildAction(buildSinks())));
        }
        return rules;
    }

    private Rule buildRule(long ruleId, Condition condition, Action action) {
        Rule rule = new Rule();
        rule.setId(ruleId);
        rule.setName("rule_" + ruleId);
        rule.setDescription("rule_" + ruleId + "_desc");
        rule.setRuleProcessorName("rule_processsor_" + ruleProcessorId);
        rule.setCondition(condition);
        rule.setAction(action);
        return rule;
    }

    private Action buildAction(List<Component> sinks) {
        Action action = new Action();
        action.setDeclaredOutput(declaredInputsOutputs);
        action.setComponents(sinks);
        return action;
    }

    private List<Component> buildSinks() {
        List<Component> sinks = new ArrayList<>();
        for (int i = 1; i <= numSinks; i++) {
            sinks.add(buildSink(i));
        }
        return sinks;
    }

    private Component buildSink(long sinkId) {
        Component sink = new Component();
        sink.setId(ruleProcessorId);
        sink.setName("sink_" + sinkId);
        sink.setDescription("sink_" + sinkId + "_desc");
        sink.setDeclaredInput(declaredInputsOutputs);
        return sink;
    }

    private Condition buildCondition(int idx) {
        if (idx % 2 == 0) {
            return buildCondition(buildConditionElements(Condition.ConditionElement.Operation.GREATER_THAN));
        }
        return buildCondition(buildConditionElements(Condition.ConditionElement.Operation.LESS_THAN));
    }

    private Condition buildCondition(List<Condition.ConditionElement> conditionElements) {
        Condition condition = new Condition();
        condition.setConditionElements(conditionElements);
        return condition;
    }

    private List<Condition.ConditionElement> buildConditionElements(Condition.ConditionElement.Operation operation) {
        List<Condition.ConditionElement> conditionElements = new ArrayList<>();
        conditionElements.add(buildConditionElement(TEMPERATURE, operation, "100", Condition.ConditionElement.LogicalOperator.AND));
        conditionElements.add(buildConditionElement(HUMIDITY, operation, "50", null));
        return conditionElements;
    }

    private Condition.ConditionElement buildConditionElement(
            String firstOperand, Condition.ConditionElement.Operation operation, String secondOperand,
            Condition.ConditionElement.LogicalOperator logicalOperator) {
        Condition.ConditionElement conditionElement =
                new Condition.ConditionElement();
        final Schema.Field temperature = new Schema.Field(firstOperand, Schema.Type.INTEGER);
        conditionElement.setFirstOperand(temperature);
        conditionElement.setOperation(operation);
        conditionElement.setSecondOperand(secondOperand);
        if (logicalOperator != null) {
            conditionElement.setLogicalOperator(logicalOperator);
        }
        return conditionElement;
    }
}
