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

package com.hortonworks.iotas.rules.condition;

import java.util.Collection;

public interface Condition {
    /** Evaluates this condition */
    boolean evaluate();

    /** @return The string representation of this condition as it is evaluated by the script language */
    String getConditionString();

    void setConditionElements(Collection<ConditionElement> conditionElements);

    /** @return The collection of condition elements that define this condition */
    Collection<ConditionElement> getConditionElements();

     /*TODO
         String s = "int x = 5; int y = 3; x > 2 && y > 1"
        Binding binding = new Binding();
        GroovyShell shell = new GroovyShell(binding);
        shell.evaluate(s)
        true
    */

    //build condition elements
    //
}
