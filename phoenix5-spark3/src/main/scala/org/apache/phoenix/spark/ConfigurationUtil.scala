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
import org.apache.hadoop.hbase.HConstants
import org.apache.phoenix.mapreduce.util.{ColumnInfoToStringEncoderDecoder, PhoenixConfigurationUtil}
import org.apache.phoenix.query.HBaseFactoryProvider
import org.apache.phoenix.util.{ColumnInfo, PhoenixRuntime}

import scala.collection.JavaConversions._

@deprecated("Use the DataSource V2 API implementation (see PhoenixDataSource)")
object ConfigurationUtil extends Serializable {

  def getOutputConfiguration(tableName: String, columns: Seq[String], zkUrl: Option[String], tenantId: Option[String] = None, conf: Option[Configuration] = None): Configuration = {

    // Create an HBaseConfiguration object from the passed in config, if present
    val config = conf match {
      case Some(c) => HBaseFactoryProvider.getConfigurationFactory.getConfiguration(c)
      case _ => HBaseFactoryProvider.getConfigurationFactory.getConfiguration()
    }

    // Set the tenantId in the config if present
    tenantId match {
      case Some(id) => setTenantId(config, id)
      case _ =>
    }

    // Set the table to save to
    PhoenixConfigurationUtil.setOutputTableName(config, tableName)
    PhoenixConfigurationUtil.setPhysicalTableName(config, tableName)
    // disable property provider evaluation
    PhoenixConfigurationUtil.setPropertyPolicyProviderDisabled(config);

    // Infer column names from the DataFrame schema
    PhoenixConfigurationUtil.setUpsertColumnNames(config, Array(columns : _*))

    // Override the Zookeeper URL if present. Throw exception if no address given.
    zkUrl match {
      case Some(url) => setZookeeperURL(config, url)
      case _ => {
        if (ConfigurationUtil.getZookeeperURL(config).isEmpty) {
          throw new UnsupportedOperationException(
            s"One of zkUrl or '${HConstants.ZOOKEEPER_QUORUM}' config property must be provided"
          )
        }
      }
    }
    // Return the configuration object
    config
  }

  def setZookeeperURL(conf: Configuration, zkUrl: String) = {
    var zk = zkUrl
    if (zk.startsWith("jdbc:phoenix:")) {
      zk = zk.substring("jdbc:phoenix:".length)
    }
    if (zk.startsWith("jdbc:phoenix+zk:")) {
      zk = zk.substring("jdbc:phoenix+zk:".length)
    }
    val escapedUrl = zk.replaceAll("\\\\:","=")
    val parts = escapedUrl.split(":")
    if (parts.length >= 1 && parts(0).length()>0)
      conf.set(HConstants.ZOOKEEPER_QUORUM, parts(0).replaceAll("=", "\\\\:"))
    if (parts.length >= 2 && parts(1).length()>0)
      conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.parseInt(parts(1).replaceAll("=", "\\\\:")))
    if (parts.length >= 3 && parts(2).length()>0)
      conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, parts(2).replaceAll("=", "\\\\:"))
  }

  def setTenantId(conf: Configuration, tenantId: String) = {
    conf.set(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId)
  }

  // Return a serializable representation of the columns
  def encodeColumns(conf: Configuration) = {
    ColumnInfoToStringEncoderDecoder.encode(conf, PhoenixConfigurationUtil.getUpsertColumnMetadataList(conf)
    )
  }

  // Decode the columns to a list of ColumnInfo objects
  def decodeColumns(conf: Configuration): List[ColumnInfo] = {
    ColumnInfoToStringEncoderDecoder.decode(conf).toList
  }

  def getZookeeperURL(conf: Configuration): Option[String] = {
    List(
      Option(conf.get(HConstants.ZOOKEEPER_QUORUM)),
      Option(conf.get(HConstants.ZOOKEEPER_CLIENT_PORT)),
      Option(conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT))
    ).flatten match {
      case Nil => None
      case x: List[String] => Some(x.mkString(":"))
    }
  }
}
