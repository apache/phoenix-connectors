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
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.phoenix.end2end.ParallelStatsEnabledIT;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.mapreduce.PhoenixInputFormat;
import org.apache.phoenix.mapreduce.PhoenixRecordWritable;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Test class for Hive PhoenixInputFormat
 */
@NotThreadSafe
public class HivePhoenixInputFormatTest extends ParallelStatsEnabledIT {
    private static final Logger LOG = LoggerFactory.getLogger(HivePhoenixInputFormatTest.class);
    /**
     * base table
     */
    private static final String BASE_TEST_TABLE = "ENTITY_HISTORY";

    /**
     * Internal test table without salting
     */
    private static final String INTERNAL_TEST_TABLE = "ENTITY_HISTORY";
    /**
     * Internal salted test table
     */
    private static final String INTERNAL_TEST_TABLE_SALTED = "ENTITY_HISTORY_SALTED";
    /**
     * Regex Pattern for custom table,the table name pattern is
     * ENTITY_HISTORY_[N_BUCKETS]_[N_GUIDEPOST]_[STAT_SWITCH]
     * and will create the table with table option:
     * SALT_BUCKETS=<N_BUCKETS>,GUIDE_POSTS_WIDTH<N_GUIDEPOST>
     * if STAT_SWITCH=0,will disable ParallelStats for the table
     */
    private static final Pattern CUSTOM_TABLE_PATTERN =
            Pattern.compile("ENTITY_HISTORY_(\\d+_\\d+_\\d+)");

    /**
     * Internal test tables name and custom test tables  for testing
     */
    private static final String[] TEST_TABLES = new String[] {
        // internal test table
        INTERNAL_TEST_TABLE,
        // internal salted table
        INTERNAL_TEST_TABLE_SALTED,
        // test table with 128 salt bucket, 128 guidepost width, parallel stat enabled
        "ENTITY_HISTORY_128_128_1",
        // test table with 128 salt bucket, 128 guidepost width, parallel stat disabled
        "ENTITY_HISTORY_128_128_0",
        // test table with 0 salt bucket, 128 guidepost width, parallel stat enabled
        "ENTITY_HISTORY_0_128_1",
        // test table with 0 salt bucket, 128 guidepost width, parallel stat disabled
        "ENTITY_HISTORY_0_128_0",
        // test table with 128 salt bucket, 1 guidepost width, parallel stat enabled
        "ENTITY_HISTORY_128_1_1",
        // test table with 128 salt bucket, 1 guidepost width, parallel stat disabled
        "ENTITY_HISTORY_128_1_0"
    };

    /**
     * This test will perform test for phoenix tables
     * with different combinations of buckets,guideposts,and parallel stat switch
     */
    @Test
    public void testGetSplitWithMultiSplitsGuidePost() throws SQLException, IOException {
        for (String table: TEST_TABLES) {
            testTable(table);
        }
    }

    /**
     * This test will perform test for phoenix tables
     * with different combinations of buckets,guideposts,and parallel stat switch
     */
    private static void testTable(String testTableName) throws SQLException, IOException {
        TableCreationInfo tableCreationInfo = getTableCreationInfo(testTableName);
        createTestTable(tableCreationInfo);
        fillTestData(tableCreationInfo, 10, 100);
        try {
            Configuration conf = getUtility().getConfiguration();
            JobConf jobConf = new JobConf(conf);
            configureTestInput(jobConf, tableCreationInfo);
            assertSameResult(jobConf, tableCreationInfo);
        } finally {
            dropTestTable(tableCreationInfo);
        }
    }

    /**
     * The function will compare the getSplits result returned by serial method and
     * parallel method and assume they are equal.
     */
    private static void assertSameResult(JobConf jobConf, TableCreationInfo creationInfo)
            throws IOException {
        final PhoenixInputFormat<PhoenixRecordWritable> inputFormat =
                new PhoenixInputFormat<PhoenixRecordWritable>();
        final int splits = creationInfo.getBuckets() > 0 ? creationInfo.getBuckets() : 1;
        InputSplit[] inputSplitsSerial;
        InputSplit[] inputSplitsParallel;

        // test get splits in parallel
        jobConf.set(PhoenixStorageHandlerConstants.PHOENIX_MINIMUM_PARALLEL_SCANS_THRESHOLD, "1");
        jobConf.set(PhoenixStorageHandlerConstants.PHOENIX_INPUTSPLIT_GENERATION_THREAD_COUNT,
                "24");
        inputSplitsParallel = inputFormat.getSplits(jobConf, splits);
        // test get splits in serial
        jobConf.set(PhoenixStorageHandlerConstants.PHOENIX_MINIMUM_PARALLEL_SCANS_THRESHOLD, "0");
        inputSplitsSerial = inputFormat.getSplits(jobConf, splits);
        System.out.println("table:"
                + creationInfo.getTableName()
                + "splits:"
                + inputSplitsSerial.length
                + "\n");
        // Test if the input split returned by serial method and parallel method are the same
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

    /**
     * The function will create internal/custom table
     * for testing.
     */
    private static void createTestTable(TableCreationInfo creationInfo) throws SQLException {
        if (creationInfo.isInternalTable()) {
            ensureTableCreated(getUrl(), creationInfo.getTableName());
        } else {
            createCustomTable(creationInfo.getTableName(), creationInfo.getBuckets(),
                    creationInfo.getGuidePost());
        }
    }
    /**
     * The function will remove custom table
     * for testing. internal tables will not be dropped
     * they will be dropped automatically after testing.
     */
    private static void dropTestTable(TableCreationInfo creationInfo) throws SQLException {
        if (creationInfo.isInternalTable()) {
            return;
        }
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            Statement statement = conn.createStatement();
            statement.execute("DROP TABLE IF EXISTS " + creationInfo.getTableName());
        }
    }
    /**
     * The function is used to fill testing data
     * for testing table
     */
    private static void fillTestData(TableCreationInfo creationInfo, int batch, int count)
            throws SQLException {
        String testTableName = creationInfo.getTableName();

        Properties props = new Properties();
        if (creationInfo.isEnableStat()) {
            props.put("phoenix.stats.guidepost.width", Long.toString(20L));
            props.put("phoenix.stats.updateFrequency", Long.toString(1L));
            props.put("phoenix.coprocessor.maxMetaDataCacheTimeToLiveMs", Long.toString(1L));
            props.put("phoenix.use.stats.parallelization", Boolean.toString(true));
        }
        Connection conn = DriverManager.getConnection(getUrl(), props);
        Date date = new Date(System.currentTimeMillis());

        try {
            try (PreparedStatement stmt = conn.prepareStatement(
                    "upsert into "
                            + testTableName
                            + "(    "
                            + "ORGANIZATION_ID,     "
                            + "PARENT_ID,     "
                            + "CREATED_DATE,     "
                            + "ENTITY_HISTORY_ID,     "
                            + "OLD_VALUE,     "
                            + "NEW_VALUE) VALUES (?, ?, ?, ?, ?, ?)"
            )) {
                for (int i = 0; i < batch; i++) {
                    for (int j = 0; j < count; j++) {
                        stmt.setString(1, UUID.randomUUID().toString().substring(0, 15));
                        stmt.setString(2, UUID.randomUUID().toString().substring(0, 15));
                        stmt.setDate(3, date);
                        stmt.setString(4, UUID.randomUUID().toString().substring(0, 15));
                        stmt.setString(5, UUID.randomUUID().toString().substring(0, 15));
                        stmt.setString(6, UUID.randomUUID().toString().substring(0, 15));
                        stmt.execute();
                    }
                    conn.commit();
                }
            }
////            //forcefully update statistics
            try (Statement stmt = conn.createStatement()) {
                final String updateStatSql = "UPDATE STATISTICS " + testTableName;
                stmt.execute(updateStatSql);
            }
        } finally {
            conn.close();
        }
    }
    /**
     * @description The function is used to create custom table with table option:
     * SALT_BUCKETS=<@bucket>,GUIDE_POSTS_WIDTH<@guidepost>
     * @param testTableName phoenix table name to create
     * @param bucket  salt bucket setting
     * @param guidepost  guidepost setting
     */
    private static void createCustomTable(String testTableName, int bucket,
            int guidepost) throws SQLException {
        String tableOptions = " SALT_BUCKETS=" + String.valueOf(bucket)
                + " ,GUIDE_POSTS_WIDTH=" + String.valueOf(guidepost);
        ensureTableCreated(getUrl(), testTableName, BASE_TEST_TABLE, null, tableOptions);
    }


    /**
     * The function is used to setup JobConf used by HivePhoenixInputFormat
     */
    protected static void configureTestInput(JobConf jobConf, TableCreationInfo creationInfo) {
        jobConf.set(PhoenixStorageHandlerConstants.PHOENIX_TABLE_NAME, creationInfo.getTableName());
        jobConf.set("ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR", "");
        jobConf.set("PhoenixStorageHandlerConstants.PHOENIX_COLUMN_MAPPING", "v1:V1,v2:V2");
        jobConf.set("phoenix.zookeeper.quorum", "localhost");
        jobConf.set("phoenix.zookeeper.client.port", String.valueOf(getZKClientPort(jobConf)));
        jobConf.set(PhoenixStorageHandlerConstants.SPLIT_BY_STATS,
                String.valueOf(creationInfo.isEnableStat()));
        jobConf.set("mapreduce.input.fileinputformat.inputdir", "/tmp");
    }

    /**
     * The function is used to parse table creation info by using tableName
     */
    public static TableCreationInfo getTableCreationInfo(String tableName) {
        TableCreationInfo info = new TableCreationInfo();
        info.setTableName(tableName);

        if (tableName.equalsIgnoreCase(INTERNAL_TEST_TABLE)) {
            info.setInternalTable(true);
            info.setBuckets(0);
            info.setGuidePost(-1);
            info.setEnableStat(true);
        } else if (tableName.equalsIgnoreCase(INTERNAL_TEST_TABLE_SALTED)) {
            info.setInternalTable(true);
            info.setBuckets(4);
            info.setGuidePost(-1);
            info.setEnableStat(true);
        } else {
            Matcher matcher = CUSTOM_TABLE_PATTERN.matcher(tableName);
            if (matcher.find()) {
                int bucket;
                int guidepost;
                int enableStat;
                String group = matcher.group(1);
                String[] params = group.split("_");
                bucket = Integer.parseInt(params[0]);
                guidepost = Integer.parseInt(params[1]);
                enableStat = Integer.parseInt(params[2]);
                info.setInternalTable(false);
                info.setBuckets(bucket);
                info.setGuidePost(guidepost);
                info.setEnableStat(enableStat > 0);
            }
        }
        return info;
    }

    /**
     * Internal Pojo Class used to store settings used for table creation.
     */
    private static class TableCreationInfo {
        private String tableName;
        private int buckets;
        private int guidePost;

        private boolean enableStat;
        private boolean isInternalTable;

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public int getBuckets() {
            return buckets;
        }

        public void setBuckets(int buckets) {
            this.buckets = buckets;
        }

        public int getGuidePost() {
            return guidePost;
        }

        public void setGuidePost(int guidePost) {
            this.guidePost = guidePost;
        }

        public boolean isEnableStat() {
            return enableStat;
        }

        public void setEnableStat(boolean enableStat) {
            this.enableStat = enableStat;
        }

        public boolean isInternalTable() {
            return isInternalTable;
        }

        public void setInternalTable(boolean internalTable) {
            isInternalTable = internalTable;
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append(tableName);
            sb.append(".");
            sb.append(buckets);
            sb.append(".");
            sb.append(guidePost);
            sb.append(".");
            sb.append(enableStat);
            return sb.toString();
        }
    }

}
