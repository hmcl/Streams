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

package com.hortonworks.iotas.layout.runtime.rule;

import com.hortonworks.iotas.layout.design.rule.exception.ConditionEvaluationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @param <I> Type of runtime input to this rule, for example {@code Tuple}
 * @param <E> Type of object required to execute this rule in the underlying streaming framework e.g {@code IOutputCollector}
 */
public interface RuleRuntime<I, E> {
    Logger logger = LoggerFactory.getLogger(RuleRuntime.class);

    /** Evaluates Condition
     *  @param input The output of a parser. Key is the field name, value is the field value
     *  @throws ConditionEvaluationException
     **/
    boolean evaluate(I input);

    /** Executes Action
     *  @param input The output of a parser. Key is the field name, value is the field value
     **/
    void execute(I input, E executor); /// storm collector.emit / spark ...
}
