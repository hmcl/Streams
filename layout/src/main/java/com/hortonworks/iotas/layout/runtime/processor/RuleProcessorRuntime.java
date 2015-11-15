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

package com.hortonworks.iotas.layout.runtime.processor;

import com.hortonworks.iotas.layout.design.component.RulesProcessor;
import com.hortonworks.iotas.layout.runtime.rule.RuleRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;


/**
 * Object representing a runtime rules processor
 * @param <I> Type of runtime input to this rule, for example {@code Tuple}
 * @param <E> Type of object required to execute this rule in the underlying streaming framework e.g {@code IOutputCollector}
 */
public class RuleProcessorRuntime<I, E> implements Serializable {
    protected static final Logger log = LoggerFactory.getLogger(RuleProcessorRuntime.class);

    protected RulesProcessor rulesProcessor;
    protected List<RuleRuntime<I,E>> rulesRuntime;

    public RuleProcessorRuntime(RuleProcessorRuntimeDependenciesBuilder<I,E> builder) {
        this.rulesProcessor = builder.getRulesProcessor();
        this.rulesRuntime = builder.getRulesRuntime();
    }

    public RulesProcessor getRulesProcessor() {
        return rulesProcessor;
    }

    public List<RuleRuntime<I, E>> getRulesRuntime() {
        return Collections.unmodifiableList(rulesRuntime);
    }

    @Override
    public String toString() {
        return "RuleProcessorRuntime{" +
                "rulesProcessor=" + rulesProcessor +
                ", rulesRuntime=" + rulesRuntime +
                '}';
    }
}

