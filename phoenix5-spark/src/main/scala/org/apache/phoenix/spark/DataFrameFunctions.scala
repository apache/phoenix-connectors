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
import org.apache.phoenix.spark.PhoenixDataFrameHelper.phoenixConfig
import org.apache.phoenix.spark.datasource.v2.PhoenixDataSource
import org.apache.spark.sql.{DataFrame, SaveMode}

@deprecated("Use the DataSource V2 API implementation (see PhoenixDataSource)")
class DataFrameFunctions(data: DataFrame) extends Serializable {
  def saveToPhoenix(parameters: Map[String, String]): Unit = {
    saveToPhoenix(
      tableName = parameters("table"),
      zkUrl = parameters.get("zkUrl"),
      tenantId = parameters.get("TenantId"),
      skipNormalizingIdentifier = parameters.contains("skipNormalizingIdentifier")
    )
  }

  def saveToPhoenix(tableName: String,
                    conf: Configuration = new Configuration,
                    zkUrl: Option[String] = None,
                    tenantId: Option[String] = None,
                    skipNormalizingIdentifier: Boolean = false): Unit = {
    data
      .write
      .format("phoenix")
      .mode(SaveMode.Overwrite)
      .option(PhoenixDataSource.TABLE, tableName)
      .option(PhoenixDataSource.JDBC_URL, zkUrl.orNull)
      .option(PhoenixDataSource.TENANT_ID, tenantId.orNull)
      .option(PhoenixDataSource.PHOENIX_CONFIGS, phoenixConfig(conf))
      .option(PhoenixDataSource.SKIP_NORMALIZING_IDENTIFIER, skipNormalizingIdentifier)
      .save()

  }

}
