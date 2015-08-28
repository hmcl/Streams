package com.hortonworks.iotas.storage.impl.jdbc.mysql.query;

import com.hortonworks.iotas.storage.StorableKey;

/**
 * Created by hlouro on 8/16/15.
 */
public class MySqlSelect extends MySqlStorableKeyBuilder {
    public MySqlSelect(String nameSpace) {
        super(nameSpace);   // super.colunns == null => no where clause filtering
    }


    public MySqlSelect(StorableKey storableKey) {
        super(storableKey);     // super.colunns != null => do where clause filtering on PrimaryKey
    }

    // "SELECT * FROM DB.TABLE [WHERE C1 = ?, C2 = ?]"
    @Override
    public String getParameterizedSql() {
        String sql = "SELECT * FROM " + tableName;
        //where clause is defined by columns specified in the PrimaryKey
        if (columns != null) {
            sql += " WHERE " + join(getColumnNames(columns, "%s = ?"), ", ");
        }

        log.debug(sql);
        return sql;
    }
}
