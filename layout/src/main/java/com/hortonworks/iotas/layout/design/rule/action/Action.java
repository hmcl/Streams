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

package com.hortonworks.iotas.layout.design.rule.action;


import com.hortonworks.iotas.layout.design.processor.Processor;

import java.util.List;

/**
 * // TODO - OUTDATED JAVADOC
 * Action that has as part of its responsibilities to emit the output (i.e. Schema - tuple for a Storm deployment)
 * that is necessary for the next component, already declared in th the layout, (e.g. HDFS sink, or another processor)
 * to be able to do its job.
 *
 * All the sinks and processors associated with this action will be evaluated with the output set by this action. The output set
 * in here becomes the input of the next Sink or Processor.
 * @param <F> {@link Schema.Field}
 **/

/** Action that is at the end of the chain of execution. Once this action is complete, this rule will not be evaluated anymore.
 *  The actions performed by this rule will not interact directly with any other components of the rule system, e.g., other rules,
 *  processors, sinks, ...
 **/

public interface Action<F> {
    /**
     * All downstream processors must receive the same input, as defined by getDeclaredOutput.
     * Actions that intend to declare different outputs must be associated with a different rule
     * @return List of downstream processors called as part this action execution
     */
    List<Processor> getProcessors();
    void setProcessors(List<Processor> processors);

    List<F> getDeclaredOutput();

    void setDeclaredOutput(List<F> declaredOutputs);
}
