/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.spark

import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark.datasource.v2.PhoenixDataSource
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable

private[spark] object PhoenixDataFrameHelper {

  def createDataFrame(table: String,
                      zkUrl: Option[String] = None,
                      tenantId: Option[String] = None,
                      conf: Configuration)(implicit sparkSession: SparkSession): DataFrame = {
    val sparkOptions = mutable.Map(PhoenixDataSource.TABLE -> table,
      PhoenixDataSource.PHOENIX_CONFIGS -> phoenixConfig(conf))

    if (zkUrl.isDefined) {
      sparkOptions += (PhoenixDataSource.JDBC_URL -> zkUrl.get)
    }
    if (tenantId.isDefined) {
      sparkOptions += (PhoenixDataSource.TENANT_ID -> tenantId.get)
    }

    sparkSession
      .read
      .format("phoenix")
      .options(sparkOptions)
      .load()
  }

  def phoenixConfig(conf: Configuration): String = {
    conf
      .iterator()
      .asScala
      .toSeq
      .filter(c => (c.getValue != null && c.getValue.trim.nonEmpty) && !c.getValue.contains(","))
      .map(c => s"${c.getKey}=${c.getValue}")
      .mkString(",")
  }

  def withSelectExpr(columns: Seq[String], df: DataFrame): DataFrame = {
    if (columns.nonEmpty) {
      df.selectExpr(columns: _*)
    } else {
      df
    }
  }

  def withWhereCondition(predicate: Option[String], df: DataFrame): DataFrame = {
    if (predicate.isDefined) {
      df.where(predicate.get)
    } else {
      df
    }
  }

}
