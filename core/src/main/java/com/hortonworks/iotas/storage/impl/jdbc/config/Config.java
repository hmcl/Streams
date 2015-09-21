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

package com.hortonworks.iotas.storage.impl.jdbc.config;

/**
 * Wrapper object that serves has a placeholder for configuration for for database connections (e.g. {$code autocommit}),
 * creation oF prepared statements (e.g. {query timeout}), etc..
 *
 * This class should be immutable as the configuration should not change after passed on to the configurable objects
 *
 **/
public class Config {
    private final int queryTimeoutSecs;

    // Replace constructors with Builder pattern when more configuration options become available
    public Config(int queryTimeoutSecs) {
        this.queryTimeoutSecs = queryTimeoutSecs;
    }

    public int getQueryTimeoutSecs() {
        return queryTimeoutSecs;
    }
}
