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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.mapreduce.PhoenixInputFormat;
import org.apache.phoenix.mapreduce.PhoenixRecordWritable;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Test class for Hive PhoenixInputFormat
 */
@NotThreadSafe
@Category(ParallelStatsDisabledTest.class)
public class HivePhoenixInputFormatTest extends ParallelStatsDisabledIT {
    private static final Logger LOG = LoggerFactory.getLogger(HivePhoenixInputFormatTest.class);
    private static final String TABLE_NAME = "HivePhoenixInputFormatTest".toUpperCase(Locale.ROOT);
    private static final String DDL = "CREATE TABLE " + TABLE_NAME
            + " (V1 varchar NOT NULL PRIMARY KEY, V2 integer)";
    private static final int SPLITS = 128;

    // This test will create phoenix table with 128 splits and compare performance of
    // serial split-generation method and parallel split-generation method.
    @Test
    public void testGetSplitsSerialOrParallel() throws IOException, SQLException {
        PhoenixInputFormat<PhoenixRecordWritable> inputFormat =
                new PhoenixInputFormat<PhoenixRecordWritable>();
        long start;
        long end;
        // create table with N splits
        System.out.println(
                String.format("generate testing table with %s splits", String.valueOf(SPLITS)));
        setupTestTable();
        // setup configuration required for PhoenixInputFormat
        Configuration conf = getUtility().getConfiguration();
        JobConf jobConf = new JobConf(conf);
        configureTestInput(jobConf);
        inputFormat.getSplits(jobConf, SPLITS);
        InputSplit[] inputSplitsSerial;
        // test get splits in serial
        start = System.currentTimeMillis();
        jobConf.set("hive.phoenix.split.parallel.threshold", "0");
        inputSplitsSerial = inputFormat.getSplits(jobConf, SPLITS);
        end = System.currentTimeMillis();
        long durationInSerial = end - start;
        System.out.println(String.format("get split in serial requires:%s ms",
                String.valueOf(durationInSerial)));

        // test get splits in parallel
        start = System.currentTimeMillis();
        jobConf.set("hive.phoenix.split.parallel.threshold", "1");
        InputSplit[] inputSplitsParallel = inputFormat.getSplits(jobConf, SPLITS);
        end = System.currentTimeMillis();
        long durationInParallel = end - start;

        System.out.println(String.format("get split in parallel requires:%s ms",
                String.valueOf(durationInParallel)));

        // Test if performance of parallel method is better than serial method
        Assert.assertTrue(durationInParallel < durationInSerial);
        // Test if the input split returned by serial method and parallel method are the same
        Assert.assertTrue(inputSplitsParallel.length == SPLITS);
        Assert.assertTrue(inputSplitsParallel.length == inputSplitsSerial.length);
        for (final InputSplit inputSplitParallel : inputSplitsParallel) {
            boolean match = false;
            for (final InputSplit inputSplitSerial : inputSplitsSerial) {
                if (inputSplitParallel.equals(inputSplitSerial)) {
                    match = true;
                    break;
                }
            }
            Assert.assertTrue(match);
        }
    }

    // create phoenix table with 128 splits.
    private static void setupTestTable() throws SQLException {
        if (SPLITS > 1) {
            final byte[] start = new byte[0];
            final byte[] end = Bytes.createMaxByteArray(4);
            final byte[][] splits = Bytes.split(start, end, SPLITS - 1);
            System.out.println(String.valueOf(splits.length));
            createTestTableWithBinarySplit(getUrl(), DDL, splits, null);
        } else {
            createTestTableWithBinarySplit(getUrl(), DDL, null, null);
        }
    }

    // build prepared sql statement with splits
    private static void buildPreparedSqlWithBinarySplits(StringBuffer sb, int splits) {
        int splitPoints = splits - 2;
        sb.append(" SPLIT ON(");
        sb.append("?");
        for (int i = 1; i < splitPoints; i++) {
            sb.append(",?");
        }
        sb.append(")");
        System.out.println(sb.toString());
    }

    // create prepared statement with params
    private static PreparedStatement createPreparedStatement(Connection connection, String newSql,
            byte[][] splitsBytes) throws SQLException {
        final PreparedStatement statement = (PreparedStatement) connection.prepareStatement(newSql);
        final int splitPoints = splitsBytes.length - 2;
        for (int i = 1; i <= splitPoints; i++) {
            statement.setBytes(i, splitsBytes[i]);
        }
        return statement;
    }

    // create phoenix test table with <SPLIT> splits and insert some testing data
    protected static void createTestTableWithBinarySplit(String url, String ddl, byte[][] splits,
            Long ts) throws SQLException {
        Properties props = new Properties();
        if (ts != null) {
            props.setProperty("CurrentSCN", Long.toString(ts));
        }
        Connection conn = DriverManager.getConnection(url, props);

        try {
            // drop table first
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            }
            if (SPLITS > 1) {
                //create table with splits
                Assert.assertNotNull(ddl);
                StringBuffer buf = new StringBuffer(ddl);
                buildPreparedSqlWithBinarySplits(buf, splits.length);
                ddl = buf.toString();
                try (PreparedStatement statement = createPreparedStatement(conn, ddl.toString(),
                        splits)) {
                    statement.execute();
                }
            } else {
                // create table without splits
                try (Statement statement = conn.createStatement()) {
                    statement.execute(ddl);
                }
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("UPSERT INTO " + TABLE_NAME + " VALUES('1' ,1)");
            }
        } catch (TableAlreadyExistsException var12) {
            throw var12;
        } finally {
            conn.close();
        }

    }

    // setup jobConf used for testing
    protected static void configureTestInput(JobConf jobConf) {
        jobConf.set(PhoenixStorageHandlerConstants.PHOENIX_TABLE_NAME, TABLE_NAME);
        jobConf.set("ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR", "");
        jobConf.set("PhoenixStorageHandlerConstants.PHOENIX_COLUMN_MAPPING", "v1:V1,v2:V2");
        jobConf.set("phoenix.zookeeper.quorum", "localhost");
        jobConf.set("phoenix.zookeeper.client.port", String.valueOf(getZKClientPort(jobConf)));
        jobConf.set("mapreduce.input.fileinputformat.inputdir", "/tmp");
    }
}
