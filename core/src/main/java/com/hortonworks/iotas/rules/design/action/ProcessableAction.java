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

package com.hortonworks.iotas.rules.design.action;

import com.hortonworks.iotas.rules.design.processor.Processor;
import com.hortonworks.iotas.rules.design.processor.Sink;

import java.util.Collection;

/** Action that has as part of its responsibilities to emit the output (i.e. Schema - tuple for a Storm deployment)
 * that is necessary for the next component, already declared in th the layout, (e.g. HDFS sink, or another processor)
 * to be able to do its job.
 *
 * All the sinks and processors associated with this action will be evaluated with the output set by this action. The output set
 * in here becomes the input of the either the Sink or Processor. The output is related to the input received by this emitted by this
 * */
public interface ProcessableAction<I> extends Action<I> {

    //TODO: Setters ???

    /**
     * @return the sinks that are going to be executed as part of this action
     */
    Collection<Sink> getSinks();

    /**
     * @return the processors that are going to be executed as part of this action
     */
    Collection<Processor> getProcessors();
}
