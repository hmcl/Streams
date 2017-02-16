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

package org.apache.streamline.cache.services.io;

import org.apache.streamline.cache.services.CacheService;

import java.util.Collection;
import java.util.Map;

/** Write/delete entries (data) to/from the underlying storage.
 *  Methods that receive a collection keys/entries should be optimized for bulk operations
 *  @param <K>   Type of the key
 *  @param <V>   Type of the value
 **/
public interface CacheWriter<K,V> extends CacheService {
    /** Write one key/val pair */
    void write(K key, V val);

    /** Write a collection of entries */
    void writeAll(Map<? extends K, ? extends V> entries);

    /** Delete one key/val pair */
    void delete(K key);

    /** Read a collection of keys */
    void deleteAll(Collection<? extends K> keys);
}
