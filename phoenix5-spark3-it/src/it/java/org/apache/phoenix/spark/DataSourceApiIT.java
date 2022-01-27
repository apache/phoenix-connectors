package org.apache.phoenix.spark;

import org.apache.phoenix.end2end.BaseQueryIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.util.Arrays;

import static org.apache.phoenix.spark.sql.connector.PhoenixDataSource.ZOOKEEPER_URL;
import static org.junit.Assert.*;

public class DataSourceApiIT extends ParallelStatsDisabledIT {

    public DataSourceApiIT() {
        super();
    }

    @Test
    public void basicWriteTest() throws SQLException {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("phoenix-test");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);
        String tableName = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl());
             Statement stmt = conn.createStatement()){
            stmt.executeUpdate("CREATE TABLE " + tableName + " (id INTEGER PRIMARY KEY, v1 VARCHAR)");
        }

        try(SparkSession spark = sqlContext.sparkSession()) {

            StructType schema = new StructType(new StructField[]{
                    new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                    new StructField("v1", DataTypes.StringType, false, Metadata.empty())
            });

            Dataset<Row> df = spark.createDataFrame(
                    Arrays.asList(
                            RowFactory.create(1, "x")),
                    schema);

            df.write()
                    .format("phoenix")
                    .mode(SaveMode.Append)
                    .option("table", tableName)
                    .option(ZOOKEEPER_URL, getUrl())
                    .save();

            try (Connection conn = DriverManager.getConnection(getUrl());
                 Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("x", rs.getString(2));
                assertFalse(rs.next());
            }


        } finally {
            jsc.stop();
        }
    }

    @Test
    @Ignore // Spark3 seems to be unable to handle mixed case colum names
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
                    .option("table", tableName)
                    .option(ZOOKEEPER_URL, getUrl())
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
