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
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlDelete;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlInsert;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlInsertUpdateDuplicate;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.MySqlSelect;
import com.hortonworks.iotas.storage.impl.jdbc.mysql.query.SqlBuilder;

public class MySqlFactory implements SqlFactory {
    @Override
    public SqlBuilder getInsertBuilder(Storable storable) {
        return new MySqlInsert(storable);
    }

    @Override
    public SqlBuilder getInsertOrUpdateBuilder(Storable storable) {
        return new MySqlInsertUpdateDuplicate(storable);
    }

    @Override
    public SqlBuilder getSelectBuilder(StorableKey storableKey) {
        return new MySqlSelect(storableKey);
    }

    @Override
    public SqlBuilder getDeleteBuilder(StorableKey storableKey) {
        return new MySqlDelete(storableKey);
    }
}
