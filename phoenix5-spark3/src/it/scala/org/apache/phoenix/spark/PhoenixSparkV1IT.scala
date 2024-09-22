/*
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package org.apache.phoenix.spark

import org.apache.phoenix.spark.sql.connector.PhoenixDataSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

import scala.collection.mutable.ListBuffer

/**
 * Note: If running directly from an IDE, these are the recommended VM parameters:
 * -Xmx1536m -XX:MaxPermSize=512m -XX:ReservedCodeCacheSize=512m
 */
class PhoenixSparkV1IT extends AbstractPhoenixSparkIT {


  test("Can persist data with case sensitive columns (like in avro schema)") {
    val df = spark.createDataFrame(
        Seq(
          (1, 1, "test_child_1"),
          (2, 1, "test_child_2"))).
      // column names are case sensitive
      toDF("ID", "TABLE3_ID", "t2col1")

    df.
      write
      .format("org.apache.phoenix.spark")
      .option(PhoenixDataSource.TABLE,"TABLE3")
      .option(PhoenixDataSource.JDBC_URL,jdbcUrl)
      .option(PhoenixDataSource.SKIP_NORMALIZING_IDENTIFIER,"true")
      .mode(SaveMode.Overwrite)
      .save()

    // Verify results
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT * FROM TABLE3")

    val checkResults = List((1, 1, "test_child_1"), (2, 1, "test_child_2"))
    val results = ListBuffer[(Long, Long, String)]()
    while (rs.next()) {
      results.append((rs.getLong(1), rs.getLong(2), rs.getString(3)))
    }
    stmt.close()

    results.toList shouldEqual checkResults
  }

  test("Can persist data with case sensitive columns using utility method") {
    val df = spark.createDataFrame(
        Seq(
          (1, 1, "test_child_1"),
          (2, 1, "test_child_2"))).
      // column names are case sensitive
      toDF("ID", "TABLE3_ID", "t2col1")
    df
      .saveToPhoenix(tableName = "TABLE3",
        zkUrl = Some(jdbcUrl),
        conf = hbaseConfiguration,
        skipNormalizingIdentifier = true)

    // Verify results
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT * FROM TABLE3")

    val checkResults = List((1, 1, "test_child_1"), (2, 1, "test_child_2"))
    val results = ListBuffer[(Long, Long, String)]()
    while (rs.next()) {
      results.append((rs.getLong(1), rs.getLong(2), rs.getString(3)))
    }
    stmt.close()

    results.toList shouldEqual checkResults
  }

  test("Can create schema RDD and execute query") {
    val df1 = spark.sqlContext.read.format("org.apache.phoenix.spark")
      .options(Map("table" -> "TABLE1", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load

    df1.createOrReplaceTempView("sql_table_1")

    val df2 = spark.sqlContext.read.format("org.apache.phoenix.spark")
      .options(Map("table" -> "TABLE2", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load

    df2.createOrReplaceTempView("sql_table_2")

    val sqlRdd = spark.sql(
      """
        |SELECT t1.ID, t1.COL1, t2.ID, t2.TABLE1_ID FROM sql_table_1 AS t1
        |INNER JOIN sql_table_2 AS t2 ON (t2.TABLE1_ID = t1.ID)""".stripMargin
    )

    val count = sqlRdd.count()

    count shouldEqual 6L
  }

  test("can read selected columns As RDD with filter") {
    val expected: Seq[Map[String, AnyRef]] = Seq(
      Map(
        "ID" -> 1.asInstanceOf[AnyRef],
        "T2COL1" -> "test_child_1".asInstanceOf[AnyRef]
      )
    )
    val rdd: RDD[Map[String, AnyRef]] = spark.sparkContext.phoenixTableAsRDD(
      table = "TABLE2",
      columns = Seq("ID", "T2COL1"),
      zkUrl = Some(quorumAddress),
      conf = hbaseConfiguration,
      predicate = Some("ID = 1"))

    rdd.collect().toSeq shouldEqual expected
  }

  test("Can persist a dataframe") {
    // Load from TABLE1
    val df = spark.sqlContext.phoenixTableAsDataFrame(
      "TABLE1",
      Nil,
      zkUrl = Some(quorumAddress),
      conf = hbaseConfiguration)
    // Save to TABLE1_COPY
    df
      .saveToPhoenix(tableName = "TABLE1_COPY", zkUrl = Some(quorumAddress), conf = hbaseConfiguration)
    // Verify results
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT * FROM TABLE1_COPY")

    val checkResults = List((1L, "test_row_1"), (2, "test_row_2"))
    val results = ListBuffer[(Long, String)]()
    while (rs.next()) {
      results.append((rs.getLong(1), rs.getString(2)))
    }
    stmt.close()

    results.toList shouldEqual checkResults
  }

}
