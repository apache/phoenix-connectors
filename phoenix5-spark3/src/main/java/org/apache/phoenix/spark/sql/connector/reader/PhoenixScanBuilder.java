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
package org.apache.phoenix.spark.sql.connector.reader;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.spark.FilterExpressionCompiler;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Tuple3;

public class PhoenixScanBuilder implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {

    private StructType schema;
    private final CaseInsensitiveStringMap options;
    private Filter[] pushedFilters = new Filter[]{};
    protected String whereClause;

    public PhoenixScanBuilder(StructType schema, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public Scan build() {
        return new PhoenixScan(schema, options, whereClause);
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        Tuple3<String, Filter[], Filter[]> tuple3 = new FilterExpressionCompiler().pushFilters(filters);
        whereClause = tuple3._1();
        pushedFilters = tuple3._3();
        return tuple3._2();
    }

    @Override
    public Filter[] pushedFilters() {
        return pushedFilters;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.schema = requiredSchema;
    }

    @VisibleForTesting
    StructType getSchema() {
        return schema;
    }

    @VisibleForTesting
    CaseInsensitiveStringMap getOptions() {
        return options;
    }

    @VisibleForTesting
    String getWhereClause() {
        return whereClause;
    }
}