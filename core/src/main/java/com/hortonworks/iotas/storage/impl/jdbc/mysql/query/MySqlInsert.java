package com.hortonworks.iotas.storage.impl.jdbc.mysql.query;

import com.hortonworks.iotas.storage.Storable;

/**
 * Created by hlouro on 8/16/15.
 */
public class MySqlInsert extends MySqlStorableBuilder {

    public MySqlInsert(Storable storable) {
        super(storable);
    }

    // "INSERT INTO DB.TABLE (id, name, age) VALUES(1, "A", 19) ON DUPLICATE KEY UPDATE id=1, name="A", age=19";
    @Override
    public String getParameterizedSql() {
        final String sql = "INSERT INTO " + tableName + " ("
                + join(getColumnNames(columns, null), ", ")
                + ") VALUES( " + getBindVariables("?, ", columns.size()) + ")";

        log.debug(sql);
        return sql;
    }
}
