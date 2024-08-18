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
import org.apache.spark.sql.{Row, SaveMode}

import scala.collection.mutable.ListBuffer

/**
 * Sub-class of PhoenixSparkIT used for tenant-specific tests
 *
 * Note: All schema related variables (table name, column names, default data, etc) are coupled with
 * phoenix-spark/src/it/resources/tenantSetup.sql
 *
 * Note: If running directly from an IDE, these are the recommended VM parameters:
 * -Xmx1536m -XX:MaxPermSize=512m -XX:ReservedCodeCacheSize=512m
 *
 */
class PhoenixSparkITTenantSpecific extends AbstractPhoenixSparkIT {

  // Tenant-specific schema info
  val OrgIdCol = "ORGANIZATION_ID"
  val TenantOnlyCol = "TENANT_ONLY_COL"
  val TenantTable = "TENANT_VIEW"
  val TenantTableWithSalt = "TENANT_VIEW_WITH_SALT"

  // Data set for tests that write to Phoenix
  val TestDataSet = List(("testOrg1", "data1"), ("testOrg2", "data2"), ("testOrg3", "data3"))
  val TestDataSet2 = List(("testOrg4", "data4"), ("testOrg5", "data5"))

  val sqlTableName = "TENANT_TABLE"

  after {
    spark.sql(s"DROP TABLE IF EXISTS  $sqlTableName")
  }

  /** ************** */
  /** Read tests * */
  /** ************** */

  test("Can read from tenant-specific table as DataFrame") {
    val expected = Array(Row.fromSeq(Seq("defaultOrg", "defaultData")))
    val df = spark.read
      .format("phoenix")
      .option(PhoenixDataSource.TABLE, TenantTable)
      .option(PhoenixDataSource.JDBC_URL, jdbcUrl)
      .option(PhoenixDataSource.TENANT_ID, TenantId)
      .load()
      .select(OrgIdCol, TenantOnlyCol)

    // There should only be 1 row upserted in tenantSetup.sql
    val result = df.collect()
    expected shouldEqual result
  }

  test("Can read from tenant-specific and salted table as DataFrame") {
    val expected = Array(Row.fromSeq(Seq("defaultOrg", "defaultData")))
    val df = spark.read
      .format("phoenix")
      .option(PhoenixDataSource.TABLE, TenantTableWithSalt)
      .option(PhoenixDataSource.JDBC_URL, jdbcUrl)
      .option(PhoenixDataSource.TENANT_ID, TenantId)
      .load()
      .select(OrgIdCol, TenantOnlyCol)

    // There should only be 1 row upserted in tenantSetup.sql
    val result = df.collect()
    expected shouldEqual result
  }

  test("Can read from tenant table using spark-sql") {

    val expected = Array(Row.fromSeq(Seq("defaultOrg", "defaultData")))

    spark.sql(s"CREATE TABLE $sqlTableName USING phoenix " +
      s"OPTIONS ('table' '$TenantTable', '${PhoenixDataSource.JDBC_URL}' '$jdbcUrl', '${PhoenixDataSource.TENANT_ID}' '${TenantId}')")

    val dataFrame = spark.sql(s"SELECT $OrgIdCol,$TenantOnlyCol FROM $sqlTableName")

    dataFrame.collect() shouldEqual expected
  }

  /** ************** */
  /** Write tests * */
  /** ************** */

  test("Can write a DataFrame using 'DataFrame.write' to tenant-specific view") {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val df = spark.sparkContext.parallelize(TestDataSet).toDF(OrgIdCol, TenantOnlyCol)
    df.write
      .format("phoenix")
      .mode(SaveMode.Append)
      .option(PhoenixDataSource.JDBC_URL, jdbcUrl)
      .option(PhoenixDataSource.TABLE, TenantTable)
      .option(PhoenixDataSource.TENANT_ID, TenantId)
      .save()

    val expected = List(("defaultOrg", "defaultData")) ::: TestDataSet
    val SelectStatement = s"SELECT $OrgIdCol,$TenantOnlyCol FROM $TenantTable"
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(SelectStatement)

    val results = ListBuffer[(String, String)]()
    while (rs.next()) {
      results.append((rs.getString(1), rs.getString(2)))
    }
    stmt.close()
    results.toList shouldEqual expected
  }

  test("Can write a DataFrame using 'DataFrame.write' to tenant-specific with salt view") {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val df = spark.sparkContext.parallelize(TestDataSet).toDF(OrgIdCol, TenantOnlyCol)
    df.write
      .format("phoenix")
      .mode(SaveMode.Append)
      .option(PhoenixDataSource.JDBC_URL, jdbcUrl)
      .option(PhoenixDataSource.TABLE, TenantTableWithSalt)
      .option(PhoenixDataSource.TENANT_ID, TenantId)
      .save()

    val expected = List(("defaultOrg", "defaultData")) ::: TestDataSet
    val SelectStatement = s"SELECT $OrgIdCol,$TenantOnlyCol FROM $TenantTableWithSalt"
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(SelectStatement)

    val results = ListBuffer[(String, String)]()
    while (rs.next()) {
      results.append((rs.getString(1), rs.getString(2)))
    }
    stmt.close()
    results.toList shouldEqual expected
  }

  test("Can use write data into tenant table using spark SQL INSERT") {
    spark.sql(s"CREATE TABLE $sqlTableName USING phoenix " +
      s"OPTIONS ('table' '$TenantTable', '${PhoenixDataSource.JDBC_URL}' '$jdbcUrl', '${PhoenixDataSource.TENANT_ID}' '${TenantId}')")

    (TestDataSet ::: TestDataSet2).foreach(tuple => {
      // Insert data
      spark.sql(s"INSERT INTO $sqlTableName VALUES('${tuple._1}', NULL, '${tuple._2}')")
    })

    val expected = List(("defaultOrg", "defaultData")) ::: TestDataSet ::: TestDataSet2
    val SelectStatement = s"SELECT $OrgIdCol,$TenantOnlyCol FROM $TenantTable"
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(SelectStatement)

    val results = ListBuffer[(String, String)]()
    while (rs.next()) {
      results.append((rs.getString(1), rs.getString(2)))
    }
    stmt.close()
    results.toList shouldEqual expected
  }

}
