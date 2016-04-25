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

package com.hortonworks.iotas.cache.view.datastore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class DataStoreWriterAsync<K, V> implements DataStoreWriter<K, V> {
    private static final int DEFAULT_NUM_THREADS = 5;

    private final DataStore<K, V> dataStore;
    private ExecutorService executorService;

    public DataStoreWriterAsync(DataStore<K, V> dataStore) {
        this(dataStore, Executors.newFixedThreadPool(DEFAULT_NUM_THREADS));
    }

    public DataStoreWriterAsync(DataStore<K, V> dataStore, ExecutorService executorService) {
        this.dataStore = dataStore;
        this.executorService = executorService;
    }

    public void write(final K key, final V val) {   //TODO val missing
        writeAll(new HashMap<K, V>() {{
            put(key, val);
        }});
    }

    public void writeAll(Map<? extends K, ? extends V> entries) {
        executorService.submit(new DataStoreWriteRunnable(entries));
    }

    public void delete(final K key) {
        deleteAll(new ArrayList<K>() {{
            add(key);
        }});
    }

    public void deleteAll(Collection<? extends K> keys) {
        executorService.submit(new DataStoreDeleteRunnable(keys));
    }

    private class DataStoreWriteRunnable implements Runnable {
        private Map<? extends K, ? extends V> entries;

        public DataStoreWriteRunnable(Map<? extends K, ? extends V> entries) {
            this.entries = entries;
        }

        @Override
        public void run() {
            dataStore.writeAll(entries);
        }
    }

    private class DataStoreDeleteRunnable implements Runnable {
        private Collection<? extends K> keys;

        public DataStoreDeleteRunnable(Collection<? extends K> keys) {
            this.keys = keys;
        }

        @Override
        public void run() {
            dataStore.deleteAll(keys);
        }
    }
}

