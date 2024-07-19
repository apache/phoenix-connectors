/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.spark.sql.connector;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.phoenix.spark.FilterExpressionCompiler;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.types.StructType;

import scala.Tuple3;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Map;

public class PhoenixDSRelation extends BaseRelation implements PrunedFilteredScan {

    private SQLContext sqlContext;
    private Map<String, String> parameters;
    private StructType schema;

    public PhoenixDSRelation(SQLContext sqlContext, Map<String, String> parameters,
            StructType schema) {
        this.sqlContext = sqlContext;
        this.parameters = parameters;
        this.schema = schema;
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public SQLContext sqlContext() {
        return sqlContext;
    }

    @Override
    public RDD<Row> buildScan(String[] requiredColumns, Filter[] filters) {

        Tuple3<String, Filter[], Filter[]> tuple3 =
        (new FilterExpressionCompiler()).pushFilters(filters);
        String whereClause = tuple3._1();

        Seq<String> reqColumnNames = JavaConverters.collectionAsScalaIterableConverter(
            Arrays.asList(requiredColumns)).asScala().toSeq();
        Seq<Column> reqColumns = JavaConverters.collectionAsScalaIterableConverter(
            Arrays.asList(requiredColumns).stream().map(n -> new Column(n))
            .collect(Collectors.toList())
                ).asScala().toSeq();

        Dataset<Row> ds = sqlContext
            .read()
            .format("phoenix")
            .options(parameters)
            .load()
            .withColumns(reqColumnNames, reqColumns);
        if(!whereClause.isEmpty()) {
            ds = ds.where(whereClause);
        }
        return ds.rdd();
     }

    @Override
    public Filter[] unhandledFilters(Filter[] filters) {
        Tuple3<String, Filter[], Filter[]> tuple3 =
                new FilterExpressionCompiler().pushFilters(filters);
        return tuple3._2();
    }

}
