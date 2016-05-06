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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

public class ParseYamlTestConfigTest {
    private static final ObjectMapper objMapYaml = new ObjectMapper(new YAMLFactory());
    private static CachesConfig cachesConfig;

    @BeforeClass
    public static void setup() throws IOException {
        cachesConfig = objMapYaml.readValue(load("cache/cache-config.yaml"), CachesConfig.class);
    }



    @Test
    public void testParseYaml() throws Exception {
        CachesConfig cachesConfig = objMapYaml
//                .addMixIn(TimeUnit.class, TimeUnitMixIn.class)
                .readValue(load("cache/cache-config.yaml"), CachesConfig.class);
//        CachesConfig cachesConfig = objMapYaml.readValue(new File("cache/cache-config.yaml"), CachesConfig.class);
        System.out.println(cachesConfig);
        System.out.println(objMapYaml.writerWithDefaultPrettyPrinter().writeValueAsString(cachesConfig));
    }

    @Test
    public void testParseYaml1() throws Exception {
        CachesConfig cachesConfig = CachesConfigYamlFactory.INSTANCE.create(loadAsStream("cache/cache-config.yaml"));
        System.out.println(cachesConfig);
        System.out.println(objMapYaml.writerWithDefaultPrettyPrinter().writeValueAsString(cachesConfig));
    }


    private static Reader load(String fileName) throws IOException {
        return new InputStreamReader(loadAsStream(fileName));
    }

    private static InputStream loadAsStream(String fileName) {
        return ParseYamlTestConfigTest.class.getClassLoader().getResourceAsStream(fileName);
    }

    /*enum MyTimeUnit {
        SECONDS("seconds"),
        MILLISECONDS("milliseconds");

        TimeUnit tu;

        private final String val;


        MyTimeUnit(String val) {
            this.val = val;
            tu  = TimeUnit.valueOf(val);
        }

        public void convert()

        @JsonCreator
        public static MyTimeUnit create(String val) {
            return val == null ? null : MyTimeUnit.valueOf(val.toUpperCase());
        }

        @JsonValue
        public String getVal() {
            return val;
        }

    }*/


}
