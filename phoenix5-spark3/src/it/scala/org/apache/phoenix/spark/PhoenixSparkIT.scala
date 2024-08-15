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


import java.sql.DriverManager
import java.util.Date
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil
import org.apache.phoenix.query.QueryServices
import org.apache.phoenix.schema.types.{PSmallintArray, PUnsignedSmallintArray, PVarchar}
import org.apache.phoenix.spark.sql.connector.{PhoenixDataSource, PhoenixTestingDataSource}
import org.apache.phoenix.spark.sql.connector.reader.PhoenixTestPartitionReader
import org.apache.phoenix.spark.sql.connector.writer.PhoenixTestBatchWrite
import org.apache.phoenix.util.{ColumnInfo, SchemaUtil}
import org.apache.spark.SparkException
import org.apache.spark.sql.types.{ArrayType, BinaryType, ByteType, DateType, IntegerType, LongType, ShortType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Note: If running directly from an IDE, these are the recommended VM parameters:
  * -Xmx1536m -XX:MaxPermSize=512m -XX:ReservedCodeCacheSize=512m
  */
class PhoenixSparkIT extends AbstractPhoenixSparkIT {

  test("Can persist data with case sensitive columns (like in avro schema)") {
    val df = spark.createDataFrame(
      Seq(
        (1, 1, "test_child_1"),
        (2, 1, "test_child_2"))).
    // column names are case sensitive
      toDF("ID", "TABLE3_ID", "t2col1")
    df.write
      .format("phoenix")
      .options(Map("table" -> "TABLE3",
        PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress, PhoenixDataSource.SKIP_NORMALIZING_IDENTIFIER -> "true"))
      .mode(SaveMode.Append)
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

  // INSERT is not support using DataSource v2 api yet
  ignore("Can use write data using spark SQL INSERT") {
    val df1 = spark.sqlContext.read.format("phoenix")
      .options( Map("table" -> "TABLE3", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load
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

  test("Can persist data into transactional tables with phoenix.transactions.enabled option") {
    var extraOptions = QueryServices.TRANSACTIONS_ENABLED + "=true";
    val df = spark.createDataFrame(
      Seq(
        (1, 1, "test_child_1"),
        (2, 1, "test_child_2"))).
      // column names are case sensitive
      toDF("ID", "TABLE5_ID", "t5col1")
    df.write
      .format("phoenix")
      .options(Map("table" -> "TABLE5",
        PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress, PhoenixDataSource.SKIP_NORMALIZING_IDENTIFIER -> "true",
        PhoenixDataSource.PHOENIX_CONFIGS -> extraOptions))
      .mode(SaveMode.Append)
      .save()


    // Verify results
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT * FROM TABLE5")

    val checkResults = List((1, 1, "test_child_1"), (2, 1, "test_child_2"))
    val results = ListBuffer[(Long, Long, String)]()
    while (rs.next()) {
      results.append((rs.getLong(1), rs.getLong(2), rs.getString(3)))
    }
    stmt.close()

    results.toList shouldEqual checkResults
  }

  test("Verify transactions in streaming and spark jobs") {
    val conn2 = DriverManager.getConnection(PhoenixSparkITHelper.getUrl)
    conn2.setAutoCommit(false)
    var stmt = conn.createStatement()
    stmt.executeUpdate("UPSERT INTO TABLE5 values(1, 1, 'test_child_0')")

    var extraOptions = QueryServices.TRANSACTIONS_ENABLED + "=true"
    val df = spark.createDataFrame(
      Seq(
        (1, 1, "test_child_1"),
        (2, 1, "test_child_2"))).
      // column names are case sensitive
      toDF("ID", "TABLE5_ID", "t5col1")
    df.write
      .format("phoenix")
      .options(Map("table" -> "TABLE5",
        PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress, PhoenixDataSource.SKIP_NORMALIZING_IDENTIFIER -> "true",
        PhoenixDataSource.PHOENIX_CONFIGS -> extraOptions))
      .mode(SaveMode.Append)
      .save()
    try {
      conn2.commit()
      fail("Abort exception should be thrown.")
    } catch {
      case e: Exception => {}
    }
    // Verify results
    stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT * FROM TABLE5")

    val checkResults = List((1, 1, "test_child_1"), (2, 1, "test_child_2"))
    val results = ListBuffer[(Long, Long, String)]()
    while (rs.next()) {
      results.append((rs.getLong(1), rs.getLong(2), rs.getString(3)))
    }
    stmt.close()

    results.toList shouldEqual checkResults
  }

  test("Can convert Phoenix schema") {
    val phoenixSchema = List(
      new ColumnInfo("varcharColumn", PVarchar.INSTANCE.getSqlType)
    )

    val catalystSchema = SparkSchemaUtil.phoenixSchemaToCatalystSchema(phoenixSchema)

    val expected = new StructType(List(StructField("varcharColumn", StringType, nullable = true)).toArray)

    catalystSchema shouldEqual expected
  }

  test("Can convert arrays of Short type in Phoenix schema") {
    val phoenixSchema = List(
      new ColumnInfo("arrayshortColumn", PSmallintArray.INSTANCE.getSqlType)
    )

    val catalystSchema = SparkSchemaUtil.phoenixSchemaToCatalystSchema(phoenixSchema)

    val expected = new StructType(List(StructField("arrayshortColumn", ArrayType(ShortType, true), nullable = true)).toArray)

    catalystSchema shouldEqual expected
  }

  test("Can create schema RDD and execute query") {
    val df1 = spark.sqlContext.read.format("phoenix")
      .options( Map("table" -> "TABLE1", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load

    df1.createOrReplaceTempView("sql_table_1")

    val df2 = spark.sqlContext.read.format("phoenix")
      .options( Map("table" -> "TABLE2", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load

    df2.createOrReplaceTempView("sql_table_2")

    val sqlRdd = spark.sql(
      """
        |SELECT t1.ID, t1.COL1, t2.ID, t2.TABLE1_ID FROM sql_table_1 AS t1
        |INNER JOIN sql_table_2 AS t2 ON (t2.TABLE1_ID = t1.ID)""".stripMargin
    )

    val count = sqlRdd.count()

    count shouldEqual 6L
  }

  ignore("Ordering by pk columns should not require sorting") {
    val df1 = spark.sqlContext.read.format("phoenix")
      .options( Map("table" -> "TABLE1", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load
    df1.createOrReplaceTempView("TABLE1")

    val sqlRdd = spark.sql("SELECT * FROM TABLE1 ORDER BY ID, COL1")
    val plan = sqlRdd.queryExecution.sparkPlan
    // verify the spark plan doesn't have a sort
    assert(!plan.toString.contains("Sort"))

    val expectedResults = Array(Row.fromSeq(Seq(1, "test_row_1")), Row.fromSeq(Seq(2, "test_row_2")))
    val actual = sqlRdd.collect()

    actual shouldEqual expectedResults
  }

  test("Verify correct number of partitions are created") {
    val conn = DriverManager.getConnection(PhoenixSparkITHelper.getUrl)
    val ddl = "CREATE TABLE SPLIT_TABLE (id VARCHAR NOT NULL PRIMARY KEY, val VARCHAR) split on ('e','j','o')"
    conn.createStatement.execute(ddl)
    val keys = Array("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s",
      "t", "u", "v", "w", "x", "y", "z")
    for (key <- keys) {
      conn.createStatement.execute("UPSERT INTO SPLIT_TABLE VALUES('" + key + "', '" + key + "')")
    }
    conn.commit()

    val df1 = spark.sqlContext.read.format("phoenix")
      .options( Map("table" -> "SPLIT_TABLE", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load
    df1.createOrReplaceTempView("SPLIT_TABLE")
    val sqlRdd = spark.sql("SELECT * FROM SPLIT_TABLE")
    val numPartitions = sqlRdd.rdd.partitions.size

    numPartitions shouldEqual 4
  }

  test("Can create schema RDD and execute query on case sensitive table (no config)") {
    val df1 = spark.sqlContext.read.format("phoenix")
      .options( Map("table" -> SchemaUtil.getEscapedArgument("table4"), PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load

    df1.createOrReplaceTempView("table4")

    val sqlRdd = spark.sql("SELECT id FROM table4")

    val count = sqlRdd.count()

    count shouldEqual 2L
  }

  test("Can create schema RDD and execute constrained query") {
    val df1 = spark.sqlContext.read.format("phoenix")
      .options( Map("table" -> "TABLE1", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load

    df1.createOrReplaceTempView("sql_table_1")

    val df2 = spark.sqlContext.read.format("phoenix")
      .options( Map("table" -> "TABLE2", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load.filter("ID = 1")

    df2.createOrReplaceTempView("sql_table_2")

    val sqlRdd = spark.sql(
      """
        |SELECT t1.ID, t1.COL1, t2.ID, t2.TABLE1_ID FROM sql_table_1 AS t1
        |INNER JOIN sql_table_2 AS t2 ON (t2.TABLE1_ID = t1.ID)""".stripMargin
    )

    val count = sqlRdd.count()

    count shouldEqual 1L
  }

  test("Can create schema RDD with predicate that will never match") {
    val df1 = spark.sqlContext.read.format("phoenix")
      .options( Map("table" -> "TABLE1", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load.filter("ID = -1")

    df1.createOrReplaceTempView("table3")

    val sqlRdd = spark.sql("SELECT * FROM table3")

    val count = sqlRdd.count()

    count shouldEqual 0L
  }

  test("Can create schema RDD with complex predicate") {
    val predicate = "ID > 0 AND TIMESERIES_KEY BETWEEN " +
      "CAST(TO_DATE('1990-01-01 00:00:01', 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) AND " +
      "CAST(TO_DATE('1990-01-30 00:00:01', 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)"
    val df1 = spark.sqlContext.read.format("phoenix")
      .options(Map("table" -> "DATE_PREDICATE_TEST_TABLE", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .load
      .filter(predicate)

    df1.createOrReplaceTempView("date_predicate_test_table")

    val sqlRdd = spark.sqlContext.sql("SELECT * FROM date_predicate_test_table")

    val count = sqlRdd.count()

    count shouldEqual 0L
  }

  test("Can query an array table") {
    val df1 = spark.sqlContext.read.format("phoenix")
      .options( Map("table" -> "ARRAY_TEST_TABLE", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load

    df1.createOrReplaceTempView("ARRAY_TEST_TABLE")

    val sqlRdd = spark.sql("SELECT * FROM ARRAY_TEST_TABLE")

    val count = sqlRdd.count()

    // get row 0, column 1, which should be "VCARRAY"
    val arrayValues = sqlRdd.collect().apply(0).apply(1)

    arrayValues should equal(Array("String1", "String2", "String3"))

    count shouldEqual 1L
  }

  test("Can read a table as an RDD") {
    val rdd1 = spark.sqlContext.read.format("phoenix")
      .options( Map("table" -> "ARRAY_TEST_TABLE", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load

    val count = rdd1.count()

    val arrayValues = rdd1.take(1)(0)(1)

    arrayValues should equal(Array("String1", "String2", "String3"))

    count shouldEqual 1L
  }

  test("Can use extraOptions to set configs for workers during reads") {
    // Pass in true, so we will get null when fetching the current row, leading to an NPE
    var extraOptions = PhoenixTestPartitionReader.RETURN_NULL_CURR_ROW + "=true"
    var rdd = spark.sqlContext.read
      .format(PhoenixTestingDataSource.TEST_SOURCE)
      .options( Map("table" -> "TABLE1", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress,
        PhoenixDataSource.PHOENIX_CONFIGS -> extraOptions)).load

    // Expect to get a NullPointerException in the executors
    var error = intercept[SparkException] {
      rdd.take(2)(0)(1)
    }
    assert(error.getCause.isInstanceOf[NullPointerException])

    // Pass in false, so we will get the expected rows
    extraOptions = PhoenixTestPartitionReader.RETURN_NULL_CURR_ROW + "=false"
    rdd = spark.sqlContext.read
      .format(PhoenixTestingDataSource.TEST_SOURCE)
      .options( Map("table" -> "TABLE1", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress,
        PhoenixDataSource.PHOENIX_CONFIGS -> extraOptions)).load
    val stringValue = rdd.take(2)(0)(1)
    stringValue shouldEqual "test_row_1"
  }

  test("Can save to phoenix table from Spark without specifying all the columns") {
    val dataSet = List(Row(1L, "1", 1), Row(2L, "2", 2), Row(3L, "3", 3))

    // COL3 is missing both from the schema and from the dataset
    val schema = StructType(
      Seq(StructField("ID", LongType, nullable = false),
        StructField("COL1", StringType),
        StructField("COL2", IntegerType)))

    val rowRDD = spark.sparkContext.parallelize(dataSet)

    // Apply the schema to the RDD.
    val df = spark.sqlContext.createDataFrame(rowRDD, schema)

    df.write
      .format("phoenix")
      .options(Map("table" -> "OUTPUT_TEST_TABLE", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .mode(SaveMode.Append)
      .save()

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT ID, COL1, COL2 FROM OUTPUT_TEST_TABLE")
    val results = ListBuffer[Row]()
    while (rs.next()) {
      results.append(Row(rs.getLong(1), rs.getString(2), rs.getInt(3)))
    }

    // Verify they match
    (0 to results.size - 1).foreach { i =>
      dataSet(i) shouldEqual results(i)
    }
  }

  test("Can use extraOptions to set configs for workers during writes") {
    val totalRecords = 100
    val upsertBatchSize = 5

    val records = new mutable.MutableList[Row]
    for (x <- 1 to totalRecords) {
      records += Row(x.toLong, x.toString, x)
    }
    val dataSet = records.toList

    val schema = StructType(
      Seq(StructField("ID", LongType, nullable = false),
        StructField("COL1", StringType),
        StructField("COL2", IntegerType)))

    // Distribute the dataset into an RDD with just 1 partition so we use only 1 executor.
    // This makes it easy to deterministically count the batched commits from that executor
    // since it corresponds to exactly 1 input partition. In case of multiple executors with
    // an uneven distribution of input partitions, if
    // (number of records in that partition) % batchSize != 0, some updates would also be committed
    // via PhoenixDataWriter#commit rather than the batch commits via PhoenixDataWriter#write
    // and those would thus, not be counted by PhoenixTestingDataWriter.
    val rowRDD = spark.sparkContext.parallelize(dataSet, 1)

    // Apply the schema to the RDD.
    val df = spark.sqlContext.createDataFrame(rowRDD, schema)
    val extraOptions =  PhoenixConfigurationUtil.UPSERT_BATCH_SIZE + "=" + upsertBatchSize.toString

    // Initially, this should be zero
    PhoenixTestBatchWrite.TOTAL_BATCHES_COMMITTED_COUNT shouldEqual 0
    df.write
      .format(PhoenixTestingDataSource.TEST_SOURCE)
      .options(Map("table" -> "OUTPUT_TEST_TABLE", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress,
        PhoenixDataSource.PHOENIX_CONFIGS -> extraOptions))
      .mode(SaveMode.Append)
      .save()

    // Verify the number of times batched updates are committed via DataWriters
    PhoenixTestBatchWrite.TOTAL_BATCHES_COMMITTED_COUNT shouldEqual totalRecords/upsertBatchSize
  }

  test("Can save dates to Phoenix using java.sql.Date") {
    val date = java.sql.Date.valueOf("2016-09-30")

    // Since we are creating a Row we have to use java.sql.date
    // java.util.date or joda.DateTime is not supported
    val dataSet = Seq(Row(1L, "1", 1, date), Row(2L, "2", 2, date))

    val schema = StructType(
      Seq(StructField("ID", LongType, nullable = false),
        StructField("COL1", StringType),
        StructField("COL2", IntegerType),
        StructField("COL3", DateType)))

    val rowRDD = spark.sparkContext.parallelize(dataSet)

    // Apply the schema to the RDD.
    val df = spark.sqlContext.createDataFrame(rowRDD, schema)

    df.write
      .format("phoenix")
      .options(Map("table" -> "OUTPUT_TEST_TABLE", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .mode(SaveMode.Append)
      .save()

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT COL3 FROM OUTPUT_TEST_TABLE WHERE ID = 1 OR ID = 2 ORDER BY ID ASC")
    val results = ListBuffer[java.sql.Date]()
    while (rs.next()) {
      results.append(rs.getDate(1))
    }

    // Verify the epochs are equal
    results(0).getTime shouldEqual date.getTime
    results(1).getTime shouldEqual date.getTime
  }

  test("Can infer schema without defining columns") {
    val df = spark.sqlContext.read.format("phoenix")
      .options( Map("table" -> "TABLE2", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load()
    df.schema("ID").dataType shouldEqual LongType
    df.schema("TABLE1_ID").dataType shouldEqual LongType
    df.schema("t2col1").dataType shouldEqual StringType
  }

  test("Spark SQL can use Phoenix as a data source with no schema specified") {
    val df = spark.sqlContext.read.format("phoenix")
      .options( Map("table" -> "TABLE1", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load
    df.count() shouldEqual 2
    df.schema("ID").dataType shouldEqual LongType
    df.schema("COL1").dataType shouldEqual StringType
  }

  // I was not able to retrieve this information for Spark3
  // The easily parsed execution plan is only logged to stdout, but is not accessible from the objects.
  ignore("Datasource v2 pushes down filters") {
    val df = spark.sqlContext.read.format("phoenix")
      .options( Map("table" -> "TABLE1", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load
    val res = df.filter(df("COL1") === "test_row_1" && df("ID") === 1L).select(df("ID"))

    // Make sure we got the right value back
    assert(res.first().getLong(0) == 1L)

    val plan = res.queryExecution.sparkPlan
    // filters should be pushed into scan
    assert(".*ScanV2 phoenix.*Filters.*ID.*COL1.*".r.findFirstIn(plan.toString).isDefined)
    // spark should not do post scan filtering
    assert(".*Filter .*ID.*COL1.*".r.findFirstIn(plan.toString).isEmpty)
  }

  test("Can persist a dataframe") {
    // Load from TABLE1
    val df = spark.sqlContext.read.format("phoenix").options( Map("table" -> "TABLE1", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load

    // Save to TABLE1_COPY
    df
      .write
      .format("phoenix")
      .mode(SaveMode.Append)
      .option("table", "TABLE1_COPY")
      .option(PhoenixDataSource.ZOOKEEPER_URL, quorumAddress)
      .save()

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

  test("Can save arrays back to phoenix") {
    val dataSet = List(Row(2L, Array("String1", "String2", "String3")))
    val schema = StructType(Seq(
      StructField("ID", LongType, nullable = false),
      StructField("VCARRAY", ArrayType(StringType, true))
    ))

    val rowRDD = spark.sparkContext.parallelize(dataSet)

    // Apply the schema to the RDD.
    val df = spark.sqlContext.createDataFrame(rowRDD, schema)

    df.write
      .format("phoenix")
      .options(Map("table" -> "ARRAY_TEST_TABLE", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .mode(SaveMode.Append)
      .save()

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT VCARRAY FROM ARRAY_TEST_TABLE WHERE ID = 2")
    rs.next()
    val sqlArray = rs.getArray(1).getArray().asInstanceOf[Array[String]]

    // Verify the arrays are equal
    sqlArray shouldEqual dataSet(0).get(1)
  }

  test("Can read from table with schema and escaped table name") {
    // Manually escape
    val df1 = spark.sqlContext.read.format("phoenix")
      .options(Map("table" -> "CUSTOM_ENTITY.\"z02\"", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load()

    var count = df1.count()

    count shouldEqual 1L

    // Use SchemaUtil
    val df2 = spark.sqlContext.read.format("phoenix")
      .options(
        Map("table" -> SchemaUtil.getEscapedFullTableName("CUSTOM_ENTITY.z02"), PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .load()

    count = df2.count()

    count shouldEqual 1L
  }

  test("Ensure DataFrame field normalization (PHOENIX-2196)") {
    val rdd1 = spark.sparkContext
      .parallelize(Seq((1L, 1L, "One"), (2L, 2L, "Two")))
      .map(p => Row(p._1, p._2, p._3))

    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("table1_id", LongType, nullable = true),
      StructField("\"t2col1\"", StringType, nullable = true)
    ))

    val df = spark.sqlContext.createDataFrame(rdd1, schema)

    df.write
      .format("phoenix")
      .options(Map("table" -> "TABLE2", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .mode(SaveMode.Append)
      .save()
  }

  test("Ensure Dataframe supports LIKE and IN filters (PHOENIX-2328)") {
    val df = spark.sqlContext.read.format("phoenix").options(Map("table" -> "TABLE1", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load()
    // Prefix match
    val res1 = df.filter("COL1 like 'test_row_%'")
    val plan = res1.groupBy().count().queryExecution.sparkPlan
    res1.count() shouldEqual 2

    // Suffix match
    val res2 = df.filter("COL1 like '%_1'")
    res2.count() shouldEqual 1
    res2.first.getString(1) shouldEqual "test_row_1"

    // Infix match
    val res3 = df.filter("COL1 like '%_row_%'")
    res3.count() shouldEqual 2

    // Not like, match none
    val res4 = df.filter("COL1 not like '%_row_%'")
    res4.count() shouldEqual 0

    // Not like, match all
    val res5 = df.filter("COL1 not like '%_wor_%'")
    res5.count() shouldEqual 2

    // "IN", match all
    val res6 = df.filter("COL1 in ('test_row_1', 'test_row_2')")
    res6.count() shouldEqual 2

    // "IN", match none
    val res7 = df.filter("COL1 in ('foo', 'bar')")
    res7.count() shouldEqual 0

    // AND (and not again)
    val res8 = df.filter("COL1 like '%_row_%' AND COL1 not like '%_1'")
    res8.count() shouldEqual 1
    res8.first.getString(1) shouldEqual "test_row_2"

    // OR
    val res9 = df.filter("COL1 like '%_1' OR COL1 like '%_2'")
    res9.count() shouldEqual 2
  }

  test("Can load decimal types with accurate precision and scale (PHOENIX-2288)") {
    val df = spark.sqlContext.read.format("phoenix")
      .options(Map("table" -> "TEST_DECIMAL", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load()
    assert(df.select("COL1").first().getDecimal(0) == BigDecimal("123.456789").bigDecimal)
  }

  test("Can load small and tiny integer types (PHOENIX-2426)") {
    val df = spark.sqlContext.read.format("phoenix")
      .options(Map("table" -> "TEST_SMALL_TINY", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load()
    assert(df.select("COL1").first().getShort(0).toInt == 32767)
    assert(df.select("COL2").first().getByte(0).toInt == 127)
  }

  test("Can save arrays from custom dataframes back to phoenix") {
    val dataSet = List(Row(2L, Array("String1", "String2", "String3"), Array(1, 2, 3)))

    val schema = StructType(
      Seq(StructField("ID", LongType, nullable = false),
        StructField("VCARRAY", ArrayType(StringType)),
        StructField("INTARRAY", ArrayType(IntegerType))))

    val rowRDD = spark.sparkContext.parallelize(dataSet)

    // Apply the schema to the RDD.
    val df = spark.sqlContext.createDataFrame(rowRDD, schema)

    df.write
      .format("phoenix")
      .options(Map("table" -> "ARRAYBUFFER_TEST_TABLE", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .mode(SaveMode.Append)
      .save()

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT VCARRAY, INTARRAY FROM ARRAYBUFFER_TEST_TABLE WHERE ID = 2")
    rs.next()
    val stringArray = rs.getArray(1).getArray().asInstanceOf[Array[String]]
    val intArray = rs.getArray(2).getArray().asInstanceOf[Array[Int]]

    // Verify the arrays are equal
    stringArray shouldEqual dataSet(0).getAs[Array[String]](1)
    intArray shouldEqual dataSet(0).getAs[Array[Int]](2)
  }

  test("Can save arrays of AnyVal type back to phoenix") {
    val dataSet = List(Row(2L, Array(1, 2, 3), Array(1L, 2L, 3L)))

    val schema = StructType(
      Seq(StructField("ID", LongType, nullable = false),
        StructField("INTARRAY", ArrayType(IntegerType)),
        StructField("BIGINTARRAY", ArrayType(LongType))))

    val rowRDD = spark.sparkContext.parallelize(dataSet)

    // Apply the schema to the RDD.
    val df = spark.sqlContext.createDataFrame(rowRDD, schema)

    df.write
      .format("phoenix")
      .options(Map("table" -> "ARRAY_ANYVAL_TEST_TABLE", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .mode(SaveMode.Append)
      .save()

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT INTARRAY, BIGINTARRAY FROM ARRAY_ANYVAL_TEST_TABLE WHERE ID = 2")
    rs.next()
    val intArray = rs.getArray(1).getArray().asInstanceOf[Array[Int]]
    val longArray = rs.getArray(2).getArray().asInstanceOf[Array[Long]]

    // Verify the arrays are equal
    intArray shouldEqual dataSet(0).get(1)
    longArray shouldEqual dataSet(0).get(2)
  }

  test("Can save arrays of Byte type back to phoenix") {
    val dataSet = List(Row(2L, Array(1.toByte, 2.toByte, 3.toByte)))

    val schema = StructType(
      Seq(StructField("ID", LongType, nullable = false),
        StructField("BYTEARRAY", ArrayType(ByteType))))

    val rowRDD = spark.sparkContext.parallelize(dataSet)

    // Apply the schema to the RDD.
    val df = spark.sqlContext.createDataFrame(rowRDD, schema)

    df.write
      .format("phoenix")
      .options(Map("table" -> "ARRAY_BYTE_TEST_TABLE", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .mode(SaveMode.Append)
      .save()

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT BYTEARRAY FROM ARRAY_BYTE_TEST_TABLE WHERE ID = 2")
    rs.next()
    val byteArray = rs.getArray(1).getArray().asInstanceOf[Array[Byte]]

    // Verify the arrays are equal
    byteArray shouldEqual dataSet(0).get(1)
  }

  test("Can save arrays of Short type back to phoenix") {
    val dataSet = List(Row(2L, Array(1.toShort, 2.toShort, 3.toShort)))

    val schema = StructType(
      Seq(StructField("ID", LongType, nullable = false),
        StructField("SHORTARRAY", ArrayType(ShortType))))

    val rowRDD = spark.sparkContext.parallelize(dataSet)

    // Apply the schema to the RDD.
    val df = spark.sqlContext.createDataFrame(rowRDD, schema)

    df.write
      .format("phoenix")
      .options(Map("table" -> "ARRAY_SHORT_TEST_TABLE", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .mode(SaveMode.Append)
      .save()

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT SHORTARRAY FROM ARRAY_SHORT_TEST_TABLE WHERE ID = 2")
    rs.next()
    val shortArray = rs.getArray(1).getArray().asInstanceOf[Array[Short]]

    // Verify the arrays are equal
    shortArray shouldEqual dataSet(0).get(1)
  }

  test("Can save binary types back to phoenix") {
    val dataSet = List(Row(2L, Array[Byte](1), Array[Byte](1, 2, 3), Array[Array[Byte]](Array[Byte](1), Array[Byte](2))))

    val schema = StructType(
      Seq(StructField("ID", LongType, false),
        StructField("BIN", BinaryType),
        StructField("VARBIN", BinaryType),
        StructField("BINARRAY", ArrayType(BinaryType))))

    val rowRDD = spark.sparkContext.parallelize(dataSet)

    // Apply the schema to the RDD.
    val df = spark.sqlContext.createDataFrame(rowRDD, schema)

    df.write
      .format("phoenix")
      .options(Map("table" -> "VARBINARY_TEST_TABLE", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .mode(SaveMode.Append)
      .save()

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT BIN, VARBIN, BINARRAY FROM VARBINARY_TEST_TABLE WHERE ID = 2")
    rs.next()
    val byte = rs.getBytes("BIN")
    val varByte = rs.getBytes("VARBIN")
    val varByteArray = rs.getArray("BINARRAY").getArray().asInstanceOf[Array[Array[Byte]]]

    // Verify the arrays are equal
    byte shouldEqual dataSet(0).get(1)
    varByte shouldEqual dataSet(0).get(2)
    varByteArray shouldEqual dataSet(0).get(3)
  }

  test("Can load and filter Phoenix DATE columns through DataFrame API") {
    val df = spark.sqlContext.read
      .format("phoenix")
      .options(Map("table" -> "DATE_TEST", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .load
    val dt = df.select("COL1").first().getDate(0).getTime
    val epoch = new Date().getTime

    // NOTE: Spark DateType drops hour, minute, second, as per the java.sql.Date spec
    // Use 'dateAsTimestamp' option to coerce DATE to TIMESTAMP without losing resolution

    // Note that Spark also applies the timezone offset to the returned date epoch. Rather than perform timezone
    // gymnastics, just make sure we're within 24H of the epoch generated just now
    assert(Math.abs(epoch - dt) < 86400000)

    df.createOrReplaceTempView("DATE_TEST")
    val df2 = spark.sql("SELECT * FROM DATE_TEST WHERE COL1 > TO_DATE('1990-01-01 00:00:01', 'yyyy-MM-dd HH:mm:ss')")
    assert(df2.count() == 1L)
  }

  test("Filter operation doesn't work for column names containing a white space (PHOENIX-2547)") {
    val df = spark.sqlContext.read.format("phoenix")
      .options(Map("table" -> SchemaUtil.getEscapedArgument("space"), PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .load
    val res = df.filter(df.col("first name").equalTo("xyz"))
    // Make sure we got the right value back
    assert(res.collectAsList().size() == 1L)
  }

  test("Spark Phoenix cannot recognize Phoenix view fields (PHOENIX-2290)") {
    val df = spark.sqlContext.read.format("phoenix")
      .options(Map("table" -> SchemaUtil.getEscapedArgument("small"), PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .load
    df.createOrReplaceTempView("temp")

    // limitation: filter / where expressions are not allowed with "double quotes", instead of that pass it as column expressions
    // reason: if the expression contains "double quotes" then spark sql parser, ignoring evaluating .. giving to next level to handle

    val res1 = spark.sql("select * from temp where salary = '10000' ")
    assert(res1.collectAsList().size() == 1L)

    val res2 = spark.sql("select * from temp where \"salary\" = '10000' ")
    assert(res2.collectAsList().size() == 0L)

    val res3 = spark.sql("select * from temp where salary > '10000' ")
    assert(res3.collectAsList().size() == 2L)
  }

  test("Queries with small case column-names return empty result-set when working with Spark Datasource Plugin (PHOENIX-2336)") {
    val df = spark.sqlContext.read.format("phoenix")
      .options(Map("table" -> SchemaUtil.getEscapedArgument("small"), PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .load

    // limitation: filter / where expressions are not allowed with "double quotes", instead of that pass it as column expressions
    // reason: if the expression contains "double quotes" then spark sql parser, ignoring evaluating .. giving to next level to handle

    val res1 = df.filter(df.col("first name").equalTo("foo"))
    assert(res1.collectAsList().size() == 1L)

    val res2 = df.filter("\"first name\" = 'foo'")
    assert(res2.collectAsList().size() == 0L)

    val res3 = df.filter("salary = '10000'")
    assert(res3.collectAsList().size() == 1L)

    val res4 = df.filter("salary > '10000'")
    assert(res4.collectAsList().size() == 2L)
  }

  test("Can coerce Phoenix DATE columns to TIMESTAMP through DataFrame API") {
    val df = spark.sqlContext.read
      .format("phoenix")
      .options(Map("table" -> "DATE_TEST", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress, "dateAsTimestamp" -> "true"))
      .load
    val dtRes = df.select("COL1").first()
    val ts = dtRes.getTimestamp(0).getTime
    val epoch = new Date().getTime

    assert(Math.abs(epoch - ts) < 300000)
  }

  test("Can load Phoenix Time columns through DataFrame API") {
    val df = spark.sqlContext.read
      .format("phoenix")
      .options(Map("table" -> "TIME_TEST", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .load
    val time = df.select("COL1").first().getTimestamp(0).getTime
    val epoch = new Date().getTime
    assert(Math.abs(epoch - time) < 86400000)
  }

  test("can read all Phoenix data types") {
    val df = spark.sqlContext.read
      .format("phoenix")
      .options(Map("table" -> "GIGANTIC_TABLE", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .load

    df.write
      .format("phoenix")
      .options(Map("table" -> "OUTPUT_GIGANTIC_TABLE", PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress))
      .mode(SaveMode.Append)
      .save()

    df.count() shouldEqual 1
  }

  test("Skip column family name when converting schema") {
    val phoenixSchema = List(new ColumnInfo("columFamily.columnName", PVarchar.INSTANCE.getSqlType))

    val catalystSchema = SparkSchemaUtil.phoenixSchemaToCatalystSchema(phoenixSchema, doNotMapColumnFamily = true)

    val expected = new StructType(List(StructField("columnName", StringType, nullable = true)).toArray)

    catalystSchema shouldEqual expected
  }

  test("Do not skip column family name when converting schema\"") {
    val phoenixSchema = List(new ColumnInfo("columFamily.columnName", PVarchar.INSTANCE.getSqlType))

    val catalystSchema = SparkSchemaUtil.phoenixSchemaToCatalystSchema(phoenixSchema)

    val expected = new StructType(List(StructField("columFamily.columnName", StringType, nullable = true)).toArray)

    catalystSchema shouldEqual expected
  }

  test("Can read data and map column to columnName") {
    val df = spark.read.format("phoenix")
      .options(
        Map("table" -> SchemaUtil.getEscapedArgument("TABLE_WITH_COL_FAMILY"),
          "doNotMapColumnFamily" -> "true",
          PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load

    val schema = df.schema

    val expected = new StructType(List(
      StructField("ID", LongType, nullable = true),
      StructField("COL1", StringType, nullable = true)
    ).toArray)

    schema shouldEqual expected
  }

  test("Can read data and map column to colFamily.columnName") {
    val df = spark.read.format("phoenix")
      .options(
        Map("table" -> SchemaUtil.getEscapedArgument("TABLE_WITH_COL_FAMILY"),
          "doNotMapColumnFamily" -> "false",
          PhoenixDataSource.ZOOKEEPER_URL -> quorumAddress)).load

    val schema = df.schema

    val expected = new StructType(List(
      StructField("ID", LongType, nullable = true),
    StructField("DATA.COL1", StringType, nullable = true)
    ).toArray)

    schema shouldEqual expected
  }

}
