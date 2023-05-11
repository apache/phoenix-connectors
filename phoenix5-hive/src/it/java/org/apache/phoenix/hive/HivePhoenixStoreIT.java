/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive;

import org.apache.hadoop.fs.Path;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.StringUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertTrue;

/**
 * Test methods only. All supporting methods should be placed to BaseHivePhoenixStoreIT
 */
@Ignore("This class contains only test methods and should not be executed directly")
public abstract class HivePhoenixStoreIT extends BaseHivePhoenixStoreIT {

    /**
     * Create a table with two column, insert 1 row, check that phoenix table is created and
     * the row is there
     *
     * @throws Exception
     */
    @Test
    public void simpleTest() throws Exception {
        String testName = "simpleTest";
        utility.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());
        createFile(StringUtil.EMPTY_STRING, new Path(hiveOutputDir, testName + ".out").toString());
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE EXTERNAL TABLE phoenix_table(ID STRING, SALARY STRING)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF + " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.table.name'='phoenix_table'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                utility.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id');");
        sb.append("INSERT INTO TABLE phoenix_table" + HiveTestUtil.CRLF +
                "VALUES ('10', '1000');" + HiveTestUtil.CRLF);
        String fullPath = new Path(utility.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);

        String phoenixQuery = "SELECT * FROM phoenix_table";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                PreparedStatement statement = conn.prepareStatement(phoenixQuery)) {
            conn.setAutoCommit(true);
            ResultSet rs = statement.executeQuery();
            assert (rs.getMetaData().getColumnCount() == 2);
            assertTrue(rs.next());
            assert (rs.getString(1).equals("10"));
            assert (rs.getString(2).equals("1000"));
        }
    }

    /**
     * Create hive table with custom column mapping
     * @throws Exception
     */

    @Test
    public void simpleColumnMapTest() throws Exception {
        String testName = "cmTest";
        utility.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());
        createFile(StringUtil.EMPTY_STRING, new Path(hiveOutputDir, testName + ".out").toString());
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE EXTERNAL TABLE column_table(ID STRING, P1 STRING, p2 STRING)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF + " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.table.name'='column_table'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.column.mapping' = 'id:C1, p1:c2, p2:C3'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                utility.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id');");
        sb.append("INSERT INTO TABLE column_table" + HiveTestUtil.CRLF +
                "VALUES ('1', '2', '3');" + HiveTestUtil.CRLF);
        String fullPath = new Path(utility.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);

        String phoenixQuery = "SELECT C1, \"c2\", C3 FROM column_table";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                PreparedStatement statement = conn.prepareStatement(phoenixQuery)) {
            conn.setAutoCommit(true);
        
            ResultSet rs = statement.executeQuery();
            assert (rs.getMetaData().getColumnCount() == 3);
            assertTrue(rs.next());
            assert (rs.getString(1).equals("1"));
            assert (rs.getString(2).equals("2"));
            assert (rs.getString(3).equals("3"));
        }
    }


    /**
     * Datatype Test
     *
     * @throws Exception
     */
    @Test
    public void dataTypeTest() throws Exception {
        String testName = "dataTypeTest";
        utility.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());
        createFile(StringUtil.EMPTY_STRING, new Path(hiveOutputDir, testName + ".out").toString());
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE EXTERNAL TABLE phoenix_datatype(ID int, description STRING, ts TIMESTAMP,  db " +
                "DOUBLE,fl FLOAT, us INT)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF + " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.table.name'='phoenix_datatype'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                utility.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id');");
        sb.append("INSERT INTO TABLE phoenix_datatype" + HiveTestUtil.CRLF +
                "VALUES (10, \"foodesc\", \"2013-01-05 01:01:01\", 200,2.0,-1);" + HiveTestUtil.CRLF);
        String fullPath = new Path(utility.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);

        String phoenixQuery = "SELECT * FROM phoenix_datatype";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                PreparedStatement statement = conn.prepareStatement(phoenixQuery)) {
            conn.setAutoCommit(true);
            ResultSet rs = statement.executeQuery();
            assert (rs.getMetaData().getColumnCount() == 6);
            while (rs.next()) {
                assert (rs.getInt(1) == 10);
                assert (rs.getString(2).equalsIgnoreCase("foodesc"));
                assert (rs.getDouble(4) == 200);
                assert (rs.getFloat(5) == 2.0);
                assert (rs.getInt(6) == -1);
            }
        }
    }

    /**
     * Datatype Test
     *
     * @throws Exception
     */
    @Test
    public void MultiKey() throws Exception {
        String testName = "MultiKey";
        utility.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());
        createFile(StringUtil.EMPTY_STRING, new Path(hiveOutputDir, testName + ".out").toString());
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE EXTERNAL TABLE phoenix_MultiKey(ID int, ID2 String,description STRING," +
                "db DOUBLE,fl FLOAT, us INT)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF +
                " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.table.name'='phoenix_MultiKey'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                utility.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id,id2');" + HiveTestUtil.CRLF);
        sb.append("INSERT INTO TABLE phoenix_MultiKey" + HiveTestUtil.CRLF +"VALUES (10, \'part2\',\'foodesc\',200,2.0,-1);" +
                HiveTestUtil.CRLF);
        String fullPath = new Path(utility.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);

        String phoenixQuery = "SELECT * FROM phoenix_MultiKey";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                PreparedStatement statement = conn.prepareStatement(phoenixQuery)) {
            conn.setAutoCommit(true);
            ResultSet rs = statement.executeQuery();
            assert (rs.getMetaData().getColumnCount() == 6);
            while (rs.next()) {
                assert (rs.getInt(1) == 10);
                assert (rs.getString(2).equalsIgnoreCase("part2"));
                assert (rs.getString(3).equalsIgnoreCase("foodesc"));
                assert (rs.getDouble(4) == 200);
                assert (rs.getFloat(5) == 2.0);
                assert (rs.getInt(6) == -1);
            }
        }
    }

    /**
     * Test that hive is able to access Phoenix data during MR job (creating two tables and perform join on it)
     *
     * @throws Exception
     */
    @Test
    public void testJoinNoColumnMaps() throws Exception {
        String testName = "testJoin";
        utility.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());
        createFile("#### A masked pattern was here ####\n10\tpart2\tfoodesc\t200.0\t2.0\t-1\t10\tpart2\tfoodesc\t200.0\t2.0\t-1\n",
                new Path(hiveOutputDir, testName + ".out").toString());
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE EXTERNAL TABLE joinTable1(ID int, ID2 String,description STRING," +
                "db DOUBLE,fl FLOAT, us INT)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF +
                " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.table.name'='joinTable1'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                utility.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id,id2');" + HiveTestUtil.CRLF);
        sb.append("CREATE EXTERNAL TABLE joinTable2(ID int, ID2 String,description STRING," +
                "db DOUBLE,fl FLOAT, us INT)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF +
                " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.table.name'='joinTable2'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                utility.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id,id2');" + HiveTestUtil.CRLF);

        sb.append("INSERT INTO TABLE joinTable1" + HiveTestUtil.CRLF +"VALUES (5, \'part2\',\'foodesc\',200,2.0,-1);" + HiveTestUtil.CRLF);
        sb.append("INSERT INTO TABLE joinTable1" + HiveTestUtil.CRLF +"VALUES (10, \'part2\',\'foodesc\',200,2.0,-1);" + HiveTestUtil.CRLF);

        sb.append("INSERT INTO TABLE joinTable2" + HiveTestUtil.CRLF +"VALUES (5, \'part2\',\'foodesc\',200,2.0,-1);" + HiveTestUtil.CRLF);
        sb.append("INSERT INTO TABLE joinTable2" + HiveTestUtil.CRLF +"VALUES (10, \'part2\',\'foodesc\',200,2.0,-1);" + HiveTestUtil.CRLF);

        sb.append("SELECT  * from joinTable1 A join joinTable2 B on A.id = B.id WHERE A.ID=10;" +
                HiveTestUtil.CRLF);

        String fullPath = new Path(utility.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);
    }

    /**
     * Test that hive is able to access Phoenix data during MR job (creating two tables and perform join on it)
     *
     * @throws Exception
     */
    @Test
    public void testJoinColumnMaps() throws Exception {
        String testName = "testJoin";
        utility.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile("#### A masked pattern was here ####\n10\t200.0\tpart2\n", new Path(hiveOutputDir, testName + ".out").toString());
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE EXTERNAL TABLE joinTable3(ID int, ID2 String,description STRING," +
                "db DOUBLE,fl FLOAT, us INT)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF +
                " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.table.name'='joinTable3'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                utility.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.column.mapping' = 'id:i1, id2:I2, db:db'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id,id2');" + HiveTestUtil.CRLF);
        sb.append("CREATE EXTERNAL TABLE joinTable4(ID int, ID2 String,description STRING," +
                "db DOUBLE,fl FLOAT, us INT)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF +
                " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.table.name'='joinTable4'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                utility.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.column.mapping' = 'id:i1, id2:I2, db:db'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id,id2');" + HiveTestUtil.CRLF);

        sb.append("INSERT INTO TABLE joinTable3" + HiveTestUtil.CRLF +"VALUES (5, \'part1\',\'foodesc\',200,2.0,-1);" + HiveTestUtil.CRLF);
        sb.append("INSERT INTO TABLE joinTable3" + HiveTestUtil.CRLF +"VALUES (10, \'part1\',\'foodesc\',200,2.0,-1);" + HiveTestUtil.CRLF);

        sb.append("INSERT INTO TABLE joinTable4" + HiveTestUtil.CRLF +"VALUES (5, \'part2\',\'foodesc\',200,2.0,-1);" + HiveTestUtil.CRLF);
        sb.append("INSERT INTO TABLE joinTable4" + HiveTestUtil.CRLF +"VALUES (10, \'part2\',\'foodesc\',200,2.0,-1);" + HiveTestUtil.CRLF);

        sb.append("SELECT A.ID, a.db, B.ID2 from joinTable3 A join joinTable4 B on A.ID = B.ID WHERE A.ID=10;" +
                HiveTestUtil.CRLF);

        String fullPath = new Path(utility.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);
        //Test that Phoenix has correctly mapped columns. We are checking both, primary key and
        // regular columns mapped and not mapped
        String phoenixQuery = "SELECT \"i1\", \"I2\", \"db\" FROM joinTable3 where \"i1\" = 10 AND \"I2\" = 'part1' AND \"db\" = 200";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                PreparedStatement statement = conn.prepareStatement(phoenixQuery)) {
            conn.setAutoCommit(true);
            ResultSet rs = statement.executeQuery();
            assert (rs.getMetaData().getColumnCount() == 3);
            while (rs.next()) {
                assert (rs.getInt(1) == 10);
                assert (rs.getString(2).equalsIgnoreCase("part1"));
                assert (rs.getDouble(3) == 200);
            }
        }
    }

    @Test
    public void testTimestampPredicate() throws Exception {
        String testName = "testTimeStampPredicate";
        utility.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile("10\t2013-01-02 01:01:01.123456\n", new Path(hiveOutputDir, testName + ".out").toString());
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE EXTERNAL TABLE timeStampTable(ID int,ts TIMESTAMP)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF +
                " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.table.name'='TIMESTAMPTABLE'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                utility.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.column.mapping' = 'id:ID, ts:TS'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id');" + HiveTestUtil.CRLF);
        /*
        Following query only for check that nanoseconds are correctly parsed with over 3 digits.
         */
        sb.append("INSERT INTO TABLE timeStampTable VALUES (10, \"2013-01-02 01:01:01.123456\");" + HiveTestUtil.CRLF);
        sb.append("SELECT * from timeStampTable WHERE ts between '2012-01-02 01:01:01.123455' and " +
                " '2015-01-02 12:01:02.123457789' AND id = 10;" + HiveTestUtil.CRLF);

        String fullPath = new Path(utility.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);
    }

    @Test
    //For some reason, this test only fails on Mapreduce, and not on Tez
    public void testCompletelyEliminateAnd() throws Exception {
        String testName = "testCompletelyEliminateAnd";
        utility.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile("1\t1\tc_h\td_h\te_h\t2020-01-30 15:10:10\t2020-01-30\n", new Path(hiveOutputDir, testName + ".out").toString());
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE EXTERNAL TABLE TEST_PHX (\n"
                + "a_h string,\n"
                + "b_h string,\n"
                + "c_h string,\n"
                + "d_h string,\n"
                + "e_h string,\n"
                + "f_h string,\n"
                + "g_h string)\n"
                + "ROW FORMAT SERDE\n"
                + "   'org.apache.phoenix.hive.PhoenixSerDe'\n"
                + "STORED BY\n"
                + "   'org.apache.phoenix.hive.PhoenixStorageHandler'\n"
                + "WITH SERDEPROPERTIES (\n"
                + "   'serialization.format'='1')\n"
                + "TBLPROPERTIES (\n"
                + "   'bucketing_version'='2',\n"
                + "'phoenix.rowkeys'='a_h, b_h, c_h, d_h, e_h, f_h' ,"
                + "'phoenix.table.name'='TEST',\n"
                + "'phoenix.zookeeper.client.port'='"+ utility.getZkCluster().getClientPort() + "',"
                + "'phoenix.zookeeper.quorum'='localhost',\n"
                + "'phoenix.zookeeper.znode.parent'='/hbase');" + HiveTestUtil.CRLF);

        sb.append("INSERT into TEST_PHX values ('1', '1', 'c_h', 'd_h', 'e_h', '2020-01-30 15:10:10', '2020-01-30');\n" + HiveTestUtil.CRLF);

        sb.append("SELECT * from TEST_PHX where f_h >=\"2020-01-30\" and b_h=\"1\";" + HiveTestUtil.CRLF);

        String fullPath = new Path(utility.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);
    }
}
