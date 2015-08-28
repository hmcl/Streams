package com.hortonworks.iotas.storage.impl.jdbc.mysql.query;

import com.hortonworks.iotas.storage.Storable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by hlouro on 8/16/15.
 */
public class MySqlInsertUpdateDuplicate extends MySqlStorableBuilder {

    public MySqlInsertUpdateDuplicate(Storable storable) {
        super(storable);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, int queryTimeoutSecs) throws SQLException {
        // the factor of 2 comes from the fact that each column is referred twice in the MySql query as
        // exemplified in the method getParameterizedSql()
        return super.doGetPreparedStatement(connection, queryTimeoutSecs, 2);
    }

    // the factor of 2 comes from the fact that each column is referred twice in the MySql query as follows
    // "INSERT INTO DB.TABLE (id, name, age) VALUES(1, "A", 19) ON DUPLICATE KEY UPDATE id=1, name="A", age=19";
    @Override
    public String getParameterizedSql() {

        String sql = "INSERT INTO " + tableName + " ("
                + join(getColumnNames(columns, null), ", ")
                + ") VALUES( " + getBindVariables("?, ", columns.size()*2) + ")"
                + " ON DUPLICATE KEY UPDATE " + join(getColumnNames(columns, "%s = ?"), ", ");

        log.debug(sql);
        return sql;
    }
}
