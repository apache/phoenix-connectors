package org.apache.phoenix.spark

import org.apache.phoenix.spark.datasource.v2.PhoenixDataSource
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
 * Note: If running directly from an IDE, these are the recommended VM parameters:
 * -Xmx1536m -XX:MaxPermSize=512m -XX:ReservedCodeCacheSize=512m
 */
class PhoenixSparkSqlIT extends AbstractPhoenixSparkIT {
  val sqlTableName = "SQL_TABLE"

  after {
    spark.sql(s"DROP TABLE IF EXISTS  $sqlTableName")
  }

  test("Can read from table using spark-sql") {
    val expected : Array[Row] = Array(
      Row.fromSeq(Seq(1, "test_row_1")),
      Row.fromSeq(Seq(2, "test_row_2"))
    )

    spark.sql(s"CREATE TABLE $sqlTableName USING phoenix " +
      s"OPTIONS ('table' 'TABLE1', '${PhoenixDataSource.JDBC_URL}' '$jdbcUrl')")

    val dataFrame = spark.sql(s"SELECT * FROM $sqlTableName")

    dataFrame.collect() shouldEqual expected
  }

  test("Can read from table using spark-sql with where clause and selecting specific columns`") {
    val expected : Array[Row] = Array(
      Row.fromSeq(Seq("test_row_1"))
    )

    spark.sql(s"CREATE TABLE $sqlTableName USING phoenix " +
      s"OPTIONS ('table' 'TABLE1', '${PhoenixDataSource.JDBC_URL}' '$jdbcUrl')")

    val dataFrame = spark.sql(s"SELECT COL1 as LABEL FROM $sqlTableName where ID=1")

    dataFrame.collect() shouldEqual expected
  }

  test("Can read from table having column family name") {
    val expected : Array[Row] = Array(
      Row.fromSeq(Seq(1))
    )

    spark.sql(s"CREATE TABLE $sqlTableName USING phoenix " +
      s"OPTIONS ('table' 'TABLE_WITH_COL_FAMILY', '${PhoenixDataSource.JDBC_URL}' '$jdbcUrl', 'doNotMapColumnFamily' 'false')")
    val dataFrame = spark.sql(s"SELECT ID FROM $sqlTableName where `DATA.COL1`='test_row_1'")

    dataFrame.collect() shouldEqual expected
  }

  test("Can read from table having column family name and map column to `columnName`") {
    val expected : Array[Row] = Array(
      Row.fromSeq(Seq(1))
    )
    spark.sql(s"CREATE TABLE $sqlTableName USING phoenix " +
      s"OPTIONS ('table' 'TABLE_WITH_COL_FAMILY', '${PhoenixDataSource.JDBC_URL}' '$jdbcUrl', 'doNotMapColumnFamily' 'true')")
    val dataFrame = spark.sql(s"SELECT ID FROM $sqlTableName where COL1='test_row_1'")

    dataFrame.collect() shouldEqual expected
  }

  test("Can use write data using spark SQL INSERT") {
    spark.sql(s"CREATE TABLE $sqlTableName USING phoenix " +
      s"OPTIONS ('table' 'TABLE3', '${PhoenixDataSource.JDBC_URL}' '$jdbcUrl', 'skipNormalizingIdentifier' 'true')")

    // Insert data
    spark.sql(s"INSERT INTO $sqlTableName VALUES(10, 10, '10')")
    spark.sql(s"INSERT INTO $sqlTableName VALUES(20, 20, '20')")

    // Verify results
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT * FROM TABLE3 WHERE ID>=10")
    val expectedResults = List((10, 10, "10"), (20, 20, "20"))
    val results = ListBuffer[(Long, Long, String)]()
    while (rs.next()) {
      results.append((rs.getLong(1), rs.getLong(2), rs.getString(3)))
    }
    stmt.close()

    results.toList shouldEqual expectedResults
  }

  // INSERT using dataFrame as init to spark-sql
  ignore("Can use write data using spark SQL INSERT using dataframe createOrReplaceTempView") {
    val df1 = spark.sqlContext.read.format("phoenix")
      .options( Map("table" -> "TABLE3", PhoenixDataSource.JDBC_URL -> this.jdbcUrl)).load
    df1.createOrReplaceTempView("TABLE3")

    // Insert data
    spark.sql("INSERT INTO TABLE3 VALUES(10, 10, 10)")
    spark.sql("INSERT INTO TABLE3 VALUES(20, 20, 20)")

    // Verify results
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT * FROM TABLE3 WHERE ID>=10")
    val expectedResults = List((10, 10, "10"), (20, 20, "20"))
    val results = ListBuffer[(Long, Long, String)]()
    while (rs.next()) {
      results.append((rs.getLong(1), rs.getLong(2), rs.getString(3)))
    }
    stmt.close()

    results.toList shouldEqual expectedResults
  }

}
