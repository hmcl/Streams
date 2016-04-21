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

package com.hortonworks.iotas.cache.view.datastore.writer;

import com.hortonworks.iotas.cache.Cache;
import com.hortonworks.iotas.cache.view.datastore.DataStore;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class DataStoreWriterAsync<K,V> implements DataStoreWriter<K,V> {
    protected Cache<K,V> cache;
    protected DataStore<K, V> dataStore;
    private ScheduledExecutorService executorService;
    private BlockingQueue<Map<? extends K, ? extends V>> batch;

    public DataStoreWriterAsync(Cache<K, V> cache, DataStore<K,V> dataStore, ScheduledExecutorService executorService) {
        this.cache = cache;
        this.dataStore = dataStore;
        this.executorService = executorService;
        this.executorService = Executors.newScheduledThreadPool(10);
        this.executorService.scheduleAtFixedRate(new Writer(), 100, 2000, TimeUnit.MILLISECONDS);
        batch = new LinkedBlockingQueue<>();
    }

    public void write(K key, V val) {   //TODO val missing
        dataStore.write(key, cache.get(key));
    }

    public void writeAll(Map<? extends K, ? extends V> entries) {
        dataStore.writeAll(entries);
    }

    public void delete(K key) {
        dataStore.delete(key);
    }

    public void deleteAll(Collection<? extends K> keys){
        dataStore.deleteAll(keys);
    }

    private class Writer implements Runnable {

        @Override
        public void run() {
//            batch.
//            dataStore.write();

        }
    }
}

