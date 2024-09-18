package org.apache.phoenix.spark.sql.connector

import org.apache.phoenix.spark.sql.connector.reader.PhoenixScanBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql._

import scala.collection.JavaConverters._

case class PhoenixSparkSqlRelation(
                                    @transient sparkSession: SparkSession,
                                    schema: StructType,
                                    params: Map[String, String]
                                  ) extends BaseRelation with PrunedFilteredScan with InsertableRelation {
  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val requiredSchema = StructType(requiredColumns.flatMap(c => schema.fields.find(_.name == c)))
    val scanBuilder: PhoenixScanBuilder = new PhoenixScanBuilder(requiredSchema, new CaseInsensitiveStringMap(params.asJava))
    scanBuilder.pushFilters(filters)
    val batch = scanBuilder.build().toBatch
    val rdd = new DataSourceRDD(
      sqlContext.sparkContext,
      batch.planInputPartitions(),
      batch.createReaderFactory(),
      false,
      Map.empty
    )
    rdd.map(ir => {
      val data = requiredSchema.zipWithIndex.map {
        case (structField, ordinal) =>
          structField.dataType match {
            case StringType => ir.getString(ordinal)
            case _ => ir.get(ordinal, structField.dataType)
          }
      }
      new GenericRowWithSchema(data.toArray, requiredSchema)
    })
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data
      .write
      .format("phoenix")
      .option(PhoenixDataSource.TABLE, params(PhoenixDataSource.TABLE))
      .option(PhoenixDataSource.JDBC_URL, PhoenixDataSource.getJdbcUrlFromOptions(params.asJava))
      .option(PhoenixDataSource.SKIP_NORMALIZING_IDENTIFIER,
        params.getOrElse(PhoenixDataSource.SKIP_NORMALIZING_IDENTIFIER, "false"))
      .option(PhoenixDataSource.TENANT_ID,params.getOrElse(PhoenixDataSource.TENANT_ID,null))
      .mode(SaveMode.Append)
      .save()
  }

}
