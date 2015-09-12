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

package integration.com.hortonworks.iotas.storage.jdbc;

import org.h2.tools.DeleteDbFiles;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcTest {

    /*@Before
    public void setUp() throws Exception {
        setConnectionBuilder();
        setJdbcStorageManager(connectionBuilder);
        setDevice();
        createTables();
    }

    @After
    public void tearDown() throws Exception {
        dropTables();
    }
*/

    /*public static void main(String... args) throws Exception {
        // delete the database named 'test' in the user home directory
        run();
    }*/

    @Test
    public void test() throws Exception {
        run();
    }

    @Test
    public void run1() throws Exception {
        // delete the database named 'test' in the user home directory
        String dbDir = "/Users/hlouro/Hugo/tmp/db/h2mysql_test/";
        DeleteDbFiles.execute(dbDir, "test", false);

        Class.forName("org.h2.Driver");
//        Connection conn = DriverManager.getConnection("jdbc:h2:~/test");
        Connection conn = DriverManager.getConnection("jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql_test/test;MODE=MySQL");
        PreparedStatement stat = conn.prepareStatement("create table test(id int primary key, name varchar(255))");
        stat.execute();
        stat.close();

        stat = conn.prepareStatement("insert into test values(1, 'Hello')");
        stat.executeUpdate();
        stat.close();
        conn.close();

        conn = DriverManager.getConnection("jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql_test/test;MODE=MySQL");
        stat = conn.prepareStatement("select * from test");
        ResultSet rs = stat.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getString("name"));
        }
        stat.close();
        conn.close();
    }

    private void run() throws ClassNotFoundException, SQLException {
        String dbDir = "/Users/hlouro/Hugo/tmp/db/h2mysql_test/";
        String db = dbDir + "test";

//        DeleteDbFiles.execute("~", "test", false);
        DeleteDbFiles.execute(dbDir, "test", false);

        Class.forName("org.h2.Driver");
//        Connection conn = DriverManager.getConnection("jdbc:h2:~/test");
        Connection conn = DriverManager.getConnection("jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql_test/test;MODE=MySQL");
        Statement stat = conn.createStatement();

        // this line would initialize the database
        // from the SQL script file 'init.sql'
        // stat.execute("runscript from 'init.sql'");

        stat.execute("create table test(id int primary key, name varchar(255))");
        stat.execute("insert into test values(1, 'Hello')");
        stat.close();
        conn.close();

        conn = DriverManager.getConnection("jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql_test/test;MODE=MySQL");
        stat = conn.createStatement();

        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        while (rs.next()) {
            System.out.println(rs.getString("name"));
        }
        stat.close();
        conn.close();
    }

    // == backup

    /*
    * public static void main(String... args) throws Exception {
        // delete the database named 'test' in the user home directory
        DeleteDbFiles.execute("~", "test", true);

        Class.forName("org.h2.Driver");
//        Connection conn = DriverManager.getConnection("jdbc:h2:~/test");
        Connection conn = DriverManager.getConnection("jdbc:h2:/Users/hlouro/Hugo/tmp/db/h2mysql_test/test");
        Statement stat = conn.createStatement();

        // this line would initialize the database
        // from the SQL script file 'init.sql'
        // stat.execute("runscript from 'init.sql'");

        stat.execute("create table test(id int primary key, name varchar(255))");
        stat.execute("insert into test values(1, 'Hello')");
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        while (rs.next()) {
            System.out.println(rs.getString("name"));
        }
        stat.close();
        conn.close();
    }
    * */
}
