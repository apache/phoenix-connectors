package org.apache.phoenix.spark.sql.connector

import org.apache.spark.sql.SparkSession

class PhoenixDataSourceTableCreationTest {
  /**
   * Given a Phoenix table with the following DDL and DML:
   *
   * CREATE TABLE TABLE1 (ID BIGINT NOT NULL PRIMARY KEY, COL1 VARCHAR);
   * UPSERT INTO TABLE1 (ID, COL1) VALUES (1, 'test_row_1');
   * UPSERT INTO TABLE1 (ID, COL1) VALUES (2, 'test_row_2');
   */

  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  // Case 1: Using org.apache.phoenix.spark.sql.connector.PhoenixDataSource
  def createPhoenixDataSourceTable(sparkTableName: String, zkUrl: String): Unit ={
    // Create Phoenix DataSource Spark Table using Spark SQL
    spark.sql(s"CREATE TABLE $sparkTableName USING phoenix OPTIONS ('table' 'TABLE1', 'zkUrl' '$zkUrl')")

    // Verify that the Phoenix data source table is selectable
    spark.sql(s"SELECT * FROM $sparkTableName").show()
  }

  // Case 2: Using org.apache.phoenix.spark.DefaultSource (deprecated)
  def createPhoenixDataSourceTableUsingDeprecatedCode(sparkTableName: String, zkUrl: String): Unit ={
    // Create Phoenix DataSource Spark Table using Spark SQL
    spark.sql(s"CREATE TABLE $sparkTableName USING org.apache.phoenix.spark OPTIONS ('table' 'TABLE1', 'zkUrl' '$zkUrl')")

    // Verify that the Phoenix data source table is selectable
    spark.sql(s"SELECT * FROM $sparkTableName").show()
  }

}