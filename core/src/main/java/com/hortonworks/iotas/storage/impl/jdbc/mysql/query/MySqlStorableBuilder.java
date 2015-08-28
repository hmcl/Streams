package com.hortonworks.iotas.storage.impl.jdbc.mysql.query;

import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorableKey;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

public abstract class MySqlStorableBuilder extends MySqlBuilder {
    protected Storable storable;

    public MySqlStorableBuilder(Storable storable) {
        this.storable = storable;
        final StorableKey storableKey = storable.getStorableKey();
        tableName = storableKey.getNameSpace();
        columns = storable.getSchema().getFields();
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, int queryTimeoutSecs) throws SQLException {
        return doGetPreparedStatement(connection, queryTimeoutSecs, 1);
    }

    protected PreparedStatement doGetPreparedStatement(Connection connection, int queryTimeoutSecs, int nTimes) throws SQLException {
        final PreparedStatement preparedStatement = prepareStatement(connection, queryTimeoutSecs);

        final int len = columns.size();
        final Map columnsToValues = storable.toMap();

        for (int j = 0; j < len * nTimes; j++) {
            Schema.Field column = columns.get(j % len);
            Schema.Type javaType = column.getType();
            String columnName = column.getName();
            setPreparedStatementParams(preparedStatement, javaType, j % len, columnsToValues.get(columnName));
        }

        return preparedStatement;
    }
}
