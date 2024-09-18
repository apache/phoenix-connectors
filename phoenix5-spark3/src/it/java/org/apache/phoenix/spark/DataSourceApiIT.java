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
package org.apache.phoenix.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.spark.sql.connector.PhoenixDataSource;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.*;
import java.util.Arrays;

import static org.apache.phoenix.spark.sql.connector.PhoenixDataSource.JDBC_URL;
import static org.apache.phoenix.spark.sql.connector.PhoenixDataSource.ZOOKEEPER_URL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_ZK;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.junit.Assert.*;

@Category(ParallelStatsDisabledTest.class)
public class DataSourceApiIT extends ParallelStatsDisabledIT {

    public DataSourceApiIT() {
        super();
    }

    @BeforeClass
    public static synchronized void setupInstanceResolver() {
        //FIXME This should be called whenever we set up a miniCluster, deep in BaseTest
        InstanceResolver.clearSingletons();
        // Make sure the ConnectionInfo in the tool doesn't try to pull a default Configuration
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
            @Override
            public Configuration getConfiguration() {
                return new Configuration(config);
            }

            @Override
            public Configuration getConfiguration(Configuration confToClone) {
                Configuration copy = new Configuration(config);
                copy.addResource(confToClone);
                return copy;
            }
        });
    }

    @Test
    public void basicWriteAndReadBackTest() throws SQLException {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("phoenix-test");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);
        String tableName = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(
                "CREATE TABLE " + tableName + " (id INTEGER PRIMARY KEY, v1 VARCHAR)");
        }

        try (SparkSession spark = sqlContext.sparkSession()) {

            StructType schema =
                    new StructType(new StructField[] {
                            new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                            new StructField("v1", DataTypes.StringType, false, Metadata.empty()) });

            // Use old zkUrl
            Dataset<Row> df1 =
                    spark.createDataFrame(
                        Arrays.asList(RowFactory.create(1, "x")),
                        schema);

            df1.write().format("phoenix").mode(SaveMode.Append)
            .option(PhoenixDataSource.TABLE, tableName)
            .option(ZOOKEEPER_URL, getUrl())
            .save();

            // Use jdbcUrl
            // In Phoenix 5.2+ getUrl() return a JDBC URL, in earlier versions it returns a ZK
            // quorum
            String jdbcUrl = getUrl();
            if (!jdbcUrl.startsWith(JDBC_PROTOCOL)) {
                jdbcUrl = JDBC_PROTOCOL_ZK + JDBC_PROTOCOL_SEPARATOR + jdbcUrl;
            }

            Dataset<Row> df2 =
                    spark.createDataFrame(
                        Arrays.asList(RowFactory.create(2, "x")),
                        schema);

            df2.write().format("phoenix").mode(SaveMode.Append)
                .option(PhoenixDataSource.TABLE, tableName)
                .option(JDBC_URL, jdbcUrl)
                .save();

            // Use default from hbase-site.xml
            Dataset<Row> df3 =
                    spark.createDataFrame(
                        Arrays.asList(RowFactory.create(3, "x")),
                        schema);

            df3.write().format("phoenix").mode(SaveMode.Append)
                .option(PhoenixDataSource.TABLE, tableName)
                .save();

            try (Connection conn = DriverManager.getConnection(getUrl());
                    Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("x", rs.getString(2));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals("x", rs.getString(2));
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertEquals("x", rs.getString(2));
                assertFalse(rs.next());
            }

            Dataset df1Read = spark.read().format("phoenix")
                    .option(PhoenixDataSource.TABLE, tableName)
                    .option(PhoenixDataSource.JDBC_URL, getUrl()).load();

            assertEquals(3l, df1Read.count());

            // Use jdbcUrl
            Dataset df2Read = spark.read().format("phoenix")
                    .option(PhoenixDataSource.TABLE, tableName)
                    .option(PhoenixDataSource.JDBC_URL, jdbcUrl)
                    .load();

            assertEquals(3l, df2Read.count());

            // Use default
            Dataset df3Read = spark.read().format("phoenix")
                    .option(PhoenixDataSource.TABLE, tableName)
                    .load();

            assertEquals(3l, df3Read.count());

        } finally {
            jsc.stop();
        }
    }

    @Test
    public void lowerCaseWriteTest() throws SQLException {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("phoenix-test");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);
        String tableName = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl());
             Statement stmt = conn.createStatement()){
            stmt.executeUpdate("CREATE TABLE " + tableName + " (id INTEGER PRIMARY KEY, v1 VARCHAR, \"v1\" VARCHAR)");
        }

        try(SparkSession spark = sqlContext.sparkSession()) {
            //Doesn't help
            spark.conf().set("spark.sql.caseSensitive", true);

            StructType schema = new StructType(new StructField[]{
                    new StructField("ID", DataTypes.IntegerType, false, Metadata.empty()),
                    new StructField("V1", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("\"v1\"", DataTypes.StringType, false, Metadata.empty())
            });

            Dataset<Row> df = spark.createDataFrame(
                    Arrays.asList(
                            RowFactory.create(1, "x", "y")),
                    schema);

            df.write()
                    .format("phoenix")
                    .mode(SaveMode.Append)
                    .option(PhoenixDataSource.TABLE, tableName)
                    .option(PhoenixDataSource.SKIP_NORMALIZING_IDENTIFIER,"true")
                    .option(JDBC_URL, getUrl())
                    .save();

            try (Connection conn = DriverManager.getConnection(getUrl());
                 Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("x", rs.getString(2));
                assertEquals("y", rs.getString(3));
                assertFalse(rs.next());
            }


        } finally {
            jsc.stop();
        }
    }

}
