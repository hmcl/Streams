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

package com.hortonworks.iotas.layout.runtime.rule.topology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.hortonworks.iotas.common.IotasEvent;
import com.hortonworks.iotas.common.IotasEventImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class RulesTestSpout extends BaseRichSpout {
    protected static final Logger log = LoggerFactory.getLogger(RulesTestSpout.class);

    private SpoutOutputCollector collector;

    public static final IotasEventImpl IOTAS_EVENT = new IotasEventImpl(new HashMap<String, Object>() {{
        put("temperature", 99);
        put("humidity", 51);
    }}, "dataSrcId", "23");

//    private static final Values VALUES = new Values(100, 50);
    private static final Values VALUES = new Values(IOTAS_EVENT);

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        log.debug("++++++++ OPENING");
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        log.debug("++++++++ Emitting Tuple: [{}]", VALUES);
        collector.emit(VALUES);
        Thread.yield();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        log.debug("++++++++ DECLARING OUPTPUT FIELDS");
        declarer.declare(getOutputFields());
    }

    public Fields getOutputFields() {
        return new Fields(IotasEvent.IOTAS_EVENT);
    }

    @Override
    public void close() {
        log.debug("++++++++ CLOSING");
        super.close();
    }
}