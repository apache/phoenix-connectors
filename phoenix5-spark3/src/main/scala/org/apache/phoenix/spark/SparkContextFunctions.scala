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
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

@deprecated("Use the DataSource V2 API implementation (see PhoenixDataSource)")
class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  /*
    This will return an RDD of Map[String, AnyRef], where the String key corresponds to the column
    name and the AnyRef value will be a java.sql type as returned by Phoenix

    'table' is the corresponding Phoenix table
    'columns' is a sequence of of columns to query
    'predicate' is a set of statements to go after a WHERE clause, e.g. "TID = 123"
    'zkUrl' is an optional Zookeeper URL to use to connect to Phoenix
    'conf' is a Hadoop Configuration object. If zkUrl is not set, the "hbase.zookeeper.quorum"
      property will be used
   */

  def phoenixTableAsRDD(table: String,
                        columns: Seq[String],
                        predicate: Option[String] = None,
                        zkUrl: Option[String] = None,
                        tenantId: Option[String] = None,
                        conf: Configuration = new Configuration()): RDD[Map[String, AnyRef]] = {

    val sparkSession = SparkSession.builder().config(sc.getConf).getOrCreate()
    val dataFrame = sparkSession.sqlContext.phoenixTableAsDataFrame(
      table = table,
      columns = columns,
      predicate = predicate,
      zkUrl = zkUrl,
      tenantId = tenantId,
      conf = conf
    )

    dataFrame
      .rdd
      .map(row => row.getValuesMap(row.schema.fieldNames))
  }
}