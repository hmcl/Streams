/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.hortonworks.iotas.cache.view.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public interface TypeConfig {
    enum Cache {
        @JsonProperty("redis")
        REDIS,
        @JsonProperty("guava")
        GUAVA,
    }

    enum RedisDatatype {
        @JsonProperty("strings")
        STRINGS,
        @JsonProperty("hashes")
        HASHES;
    }

    enum DataStore {
        @JsonProperty("phoenix")
        PHOENIX,
        @JsonProperty("mysql")
        MYSQL,
        @JsonProperty("hbase")
        HBASE;
    }

    enum CacheLoader {
        @JsonProperty("sync")
        SYNC,
        @JsonProperty("async")
        ASYNC;
    }

    enum CacheReader {
        @JsonProperty("through")
        THROUGH
    }

    enum CacheWriter {
        @JsonProperty("sync")
        SYNC,
        @JsonProperty("async")
        ASYNC;
    }
}
