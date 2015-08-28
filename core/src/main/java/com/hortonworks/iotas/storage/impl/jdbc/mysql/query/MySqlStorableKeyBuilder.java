package com.hortonworks.iotas.storage.impl.jdbc.mysql.query;

import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.storage.PrimaryKey;
import com.hortonworks.iotas.storage.StorableKey;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;

public abstract class MySqlStorableKeyBuilder extends MySqlBuilder {
    protected PrimaryKey primaryKey;

    public MySqlStorableKeyBuilder(String nameSpace) {
        tableName = nameSpace;
    }

    public MySqlStorableKeyBuilder(StorableKey storableKey) {
        tableName = storableKey.getNameSpace();
        primaryKey = storableKey.getPrimaryKey();
        columns = new LinkedList<>(storableKey.getPrimaryKey().getFieldsToVal().keySet());
        Collections.sort(columns);  // Sorting is needed because keySet() view does not guarantee ordering TODO: ???
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, int queryTimeoutSecs) throws SQLException {
        return doGetPreparedStatement(connection, queryTimeoutSecs, 1);
    }

    protected PreparedStatement doGetPreparedStatement(Connection connection, int queryTimeoutSecs, int nTimes) throws SQLException {
        final PreparedStatement preparedStatement = prepareStatement(connection, queryTimeoutSecs);

        final int len = columns.size();
        Map<Schema.Field, Object> columnsToValues = primaryKey.getFieldsToVal();


        for (int j = 0; j < len * nTimes; j++) {
            Schema.Field column = columns.get(j % len);
            Schema.Type javaType = column.getType();
            setPreparedStatementParams(preparedStatement, javaType, j % len, columnsToValues.get(column));
        }

        return preparedStatement;
    }
}
