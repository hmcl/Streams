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

package com.hortonworks.iotas.storage.impl.jdbc.mysql.factory;

import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorableKey;
import com.hortonworks.iotas.storage.exception.NonIncrementalColumnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Collection;

public interface SqlExecutor {
    Logger log = LoggerFactory.getLogger(SqlExecutor.class);

    void insert(Storable storable);

    void insertOrUpdate(Storable storable);

    void delete(StorableKey storableKey);

    /** @return all entries in the given namespace */
    <T extends Storable> Collection<T> select(String namespace);

    /** @return all entries that match the specified {@link StorableKey} */
    <T extends Storable> Collection<T> select(StorableKey storableKey);

    /**
     * @return The next availabe id for the autoincrement column in the specified {@code namespace}
     * @exception NonIncrementalColumnException if {@code namespace} has no autoincrement column
     * */
    Long nextId(String namespace);

    /** @return an active connection to the underlying storage queried by the methods in this interface */
    Connection getConnection();
}
