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

package com.hortonworks.iotas.rules.action;

import com.hortonworks.iotas.common.Schema;

/** Action that has as part of its responsibilities to emit the output (i.e. Schema - tuple for in a Storm deployment)
 * that is necessary for the next component (Storm Bolt) declared in th the layout to be able to do its job.
 * A Storm Bolt is an example of a Processor. A rule of this type will always cause the next processor in the chain
 * to be executed and receive the declared output.
 * */
public interface ProcessorAction {
    void execute();
    Schema getOutputSchema();
}
