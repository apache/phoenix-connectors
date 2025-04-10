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

import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark.datasource.v2.PhoenixDataSource
import org.apache.phoenix.util.PhoenixRuntime
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import java.util.Properties
import scala.collection.JavaConverters.{asScalaIteratorConverter, mapAsJavaMapConverter}

@deprecated("Use the DataSource V2 API implementation (see PhoenixDataSource)")
class ProductRDDFunctions[A <: Product](data: RDD[A]) extends Serializable {
  def saveToPhoenix(tableName: String,
                    cols: Seq[String],
                    conf: Configuration = new Configuration,
                    zkUrl: Option[String] = None,
                    tenantId: Option[String] = None): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().config(data.sparkContext.getConf).getOrCreate()
    val dsOptions = new DataSourceOptions(Map(PhoenixDataSource.JDBC_URL -> zkUrl.orNull).asJava)
    val jdbcUrl = PhoenixDataSource.getJdbcUrlFromOptions(dsOptions)
    val confAsMap = conf.iterator().asScala.map(c => (c.getKey -> c.getValue)).toMap.asJava
    val confToSet = new Properties()
    confToSet.putAll(confAsMap)
    if (tenantId.isDefined) {
      confToSet.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId.get)
    }
    val schema: StructType = SparkSchemaUtil.phoenixSchema(tableName, cols, jdbcUrl, confToSet)
    val dataFrame = sparkSession.createDataFrame(data.map(Row.fromTuple), schema).selectExpr(cols: _*)
    new DataFrameFunctions(dataFrame)
      .saveToPhoenix(
        tableName = tableName,
        conf = conf,
        zkUrl = zkUrl,
        tenantId = tenantId
      )
  }

}