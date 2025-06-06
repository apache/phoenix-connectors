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
class PhoenixSparkV1ITTenantSpecific extends AbstractPhoenixSparkIT {

  // Tenant-specific schema info
  val OrgIdCol = "ORGANIZATION_ID"
  val TenantOnlyCol = "TENANT_ONLY_COL"
  val TenantTable = "TENANT_VIEW"

  // Data set for tests that write to Phoenix
  val TestDataSet = List(("testOrg1", "data1"), ("testOrg2", "data2"), ("testOrg3", "data3"))
  val TestDataSet2 = List(("testOrg1", "data1", TenantId, "g1"), ("testOrg2", "data2", TenantId, "g3"),
    ("testOrg3", "data3", TenantId, "g3"))

  /**
    * Helper method used by write tests to verify content written.
    * Assumes the caller has written the TestDataSet (defined above) to Phoenix
    * and that 1 row of default data exists (upserted after table creation in tenantSetup.sql)
    */
  def verifyResults(): Unit = {
    // Contains the default data upserted into the tenant-specific table in tenantSetup.sql and the data upserted by tests
    val VerificationDataSet = List(("defaultOrg", "defaultData")) ::: TestDataSet

    val SelectStatement = "SELECT " + OrgIdCol + "," + TenantOnlyCol + " FROM " + TenantTable
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(SelectStatement)

    val results = ListBuffer[(String, String)]()
    while (rs.next()) {
      results.append((rs.getString(1), rs.getString(2)))
    }
    stmt.close()
    results.toList shouldEqual VerificationDataSet
  }

  /*****************/
  /** Read tests **/
  /*****************/

  test("Can read from tenant-specific table as DataFrame") {
    val df = spark.sqlContext.phoenixTableAsDataFrame(
      TenantTable,
      Seq(OrgIdCol, TenantOnlyCol),
      zkUrl = Some(quorumAddress),
      tenantId = Some(TenantId),
      conf = hbaseConfiguration)

    // There should only be 1 row upserted in tenantSetup.sql
    val count = df.count()
    count shouldEqual 1L
  }

  test("Can read from tenant-specific table as RDD") {
    val rdd = spark.sparkContext.phoenixTableAsRDD(
      TenantTable,
      Seq(OrgIdCol, TenantOnlyCol),
      zkUrl = Some(quorumAddress),
      tenantId = Some(TenantId),
      conf = hbaseConfiguration)

    // There should only be 1 row upserted in tenantSetup.sql
    val count = rdd.count()
    count shouldEqual 1L
  }

  /*****************/
  /** Write tests **/
  /*****************/

  test("Can write a DataFrame using 'DataFrame.saveToPhoenix' to tenant-specific view") {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val df = spark.sparkContext.parallelize(TestDataSet).toDF(OrgIdCol, TenantOnlyCol)
    df.saveToPhoenix(TenantTable, zkUrl = Some(quorumAddress), tenantId = Some(TenantId))

    verifyResults
  }

  test("Can write an RDD to Phoenix tenant-specific view") {
    spark.sparkContext
      .parallelize(TestDataSet)
      .saveToPhoenix(
        TenantTable,
        Seq(OrgIdCol, TenantOnlyCol),
        hbaseConfiguration,
        tenantId = Some(TenantId)
      )

    verifyResults
  }
}
