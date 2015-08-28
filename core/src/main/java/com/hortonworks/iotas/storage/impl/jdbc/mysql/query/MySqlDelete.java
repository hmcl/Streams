package com.hortonworks.iotas.storage.impl.jdbc.mysql.query;

import com.hortonworks.iotas.storage.StorableKey;

/**
 * Created by hlouro on 8/16/15.
 */
public class MySqlDelete extends MySqlStorableKeyBuilder {

    public MySqlDelete(StorableKey storableKey) {
        super(storableKey);
    }

    // "DELETE FROM DB.TABLE WHERE id1 = val1 AND id2 = val2"
    @Override
    public String getParameterizedSql() {

        final String sql = "DELETE FROM  " + tableName + " WHERE "
                + join(getColumnNames(columns, "%s = ?"), " AND ");
        log.debug(sql);
        return sql;
    }
}
