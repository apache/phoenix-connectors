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

import java.sql.{Connection, DriverManager}
import java.util.{Properties, TimeZone}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.apache.phoenix.query.BaseTest
import org.apache.phoenix.spark.datasource.v2.writer.PhoenixTestingDataSourceWriter
import org.apache.phoenix.util.PhoenixRuntime
import org.apache.phoenix.util.ReadOnlyProps
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}


// Helper object to access the protected abstract static methods hidden in BaseTest
object PhoenixSparkITHelper extends BaseTest {
  def getTestClusterConfig = new Configuration(BaseTest.config);

  def doSetup = {
    // Set-up fixed timezone for DATE and TIMESTAMP tests
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    // The @ClassRule doesn't seem to be getting picked up, force creation here before setup
    BaseTest.tmpFolder.create()
    BaseTest.setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);
  }

  def doTeardown = {
    BaseTest.dropNonSystemTables();
    BaseTest.tmpFolder.delete()
  }

  def cleanUpAfterTest = {
    BaseTest.deletePriorMetaData(HConstants.LATEST_TIMESTAMP, getUrl);
  }

  def getUrl = BaseTest.getUrl
}

/**
  * Base class for PhoenixSparkIT
  */
class AbstractPhoenixSparkIT extends FunSuite with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  // A global tenantId we can use across tests
  final val TenantId = "theTenant"

  var conn: Connection = _
  var spark: SparkSession = _

  lazy val hbaseConfiguration = {
    val conf = PhoenixSparkITHelper.getTestClusterConfig
    conf
  }

  lazy val jdbcUrl = PhoenixSparkITHelper.getUrl

  lazy val quorumAddress = PhoenixSparkITHelper.getUrl

  // Runs SQL commands located in the file defined in the sqlSource argument
  // Optional argument tenantId used for running tenant-specific SQL
  def setupTables(sqlSource: String, tenantId: Option[String]): Unit = {
    val props = new Properties
    if(tenantId.isDefined) {
      props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId.get)
    }

    conn = DriverManager.getConnection(PhoenixSparkITHelper.getUrl, props)
    conn.setAutoCommit(true)

    val setupSqlSource = getClass.getClassLoader.getResourceAsStream(sqlSource)

    // each SQL statement used to set up Phoenix must be on a single line. Yes, that
    // can potentially make large lines.
    val setupSql = scala.io.Source.fromInputStream(setupSqlSource).getLines()
      .filter(line => !line.startsWith("--") && !line.isEmpty)

    for (sql <- setupSql) {
      val stmt = conn.createStatement()
      stmt.execute(sql)
    }
    conn.commit()
  }

  before{
    // Reset batch counter after each test
    PhoenixTestingDataSourceWriter.TOTAL_BATCHES_COMMITTED_COUNT = 0
  }

  override def beforeAll() {
    PhoenixSparkITHelper.doSetup

    // We pass in null for TenantId here since these tables will be globally visible
    setupTables("globalSetup.sql", None)
    // create transactional tables
    setupTables("transactionTableSetup.sql", None)
    // We pass in a TenantId to allow the DDL to create tenant-specific tables/views
    setupTables("tenantSetup.sql", Some(TenantId))

    spark = SparkSession
      .builder()
      .appName("PhoenixSparkIT")
      .master("local[2]") // 2 threads, some parallelism
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.hadoopRDD.ignoreEmptySplits", "false")
      .getOrCreate()
  }

  override def afterAll() {
    conn.close()
    spark.stop()
    PhoenixSparkITHelper.cleanUpAfterTest
    PhoenixSparkITHelper.doTeardown
  }
}
