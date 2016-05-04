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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public interface TypeConfig {
    enum Cache {
        REDIS("redis"),
        GUAVA("guava"),
        MEMCACHED("memcached");

        private String val;

        Cache(String val) {
            this.val = val;
        }

        @JsonCreator
        public static Cache create(String val) {
            return val == null ? null : Cache.valueOf(val.toUpperCase());
        }

        @JsonValue
        public String getVal() {
            return val;
        }
    }

    enum RedisDatatype {
        STRINGS("strings"),
        HASHES("hashes");

        private String val;

        RedisDatatype(String val) {
            this.val = val;
        }

        @JsonCreator
        public static RedisDatatype create(String val) {
            return val == null ? null : RedisDatatype.valueOf(val.toUpperCase());
        }

        @JsonValue
        public String getVal() {
            return val;
        }

    }

    enum DataStore {
        PHOENIX("phoenix"),
        MYSQL("mysql"),
        HBASE("hbase");

        private String val;

        DataStore(String val) {
            this.val = val;
        }

        @JsonCreator
        public static DataStore create(String val) {
            return val == null ? null : DataStore.valueOf(val.toUpperCase());
        }

        @JsonValue
        public String getVal() {
            return val;
        }
    }

    enum CacheLoader {
        SYNC("sync"),
        ASYNC("async");

        private String val;

        CacheLoader(String val) {
            this.val = val;
        }

        @JsonCreator
        public static CacheLoader create(String val) {
            return val == null ? null : CacheLoader.valueOf(val.toUpperCase());
        }

        @JsonValue
        public String getVal() {
            return val;
        }
    }

    enum CacheReader {
        THROUGH("through");

        private String val;

        CacheReader(String val) {
            this.val = val;
        }

        @JsonCreator
        public static CacheReader create(String val) {
            return val == null ? null : CacheReader.valueOf(val.toUpperCase());
        }

        @JsonValue
        public String getVal() {
            return val;
        }
    }

    enum CacheWriter {
        SYNC("sync"),
        ASYNC("async");

        private String val;

        CacheWriter(String val) {
            this.val = val;
        }

        @JsonCreator
        public static CacheWriter create(String val) {
            return val == null ? null : CacheWriter.valueOf(val.toUpperCase());
        }

        @JsonValue
        public String getVal() {
            return val;
        }
    }
}
