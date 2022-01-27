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
package org.apache.phoenix.spark.datasource.v2;

import org.apache.phoenix.spark.SparkSchemaUtil;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

public class PhoenixTestingDataSource extends PhoenixDataSource {

    public static final String TEST_SOURCE =
            "org.apache.phoenix.spark.datasource.v2.PhoenixTestingDataSource";

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        String tableName = options.get("table");
        String zkUrl = options.get(ZOOKEEPER_URL);
        boolean dateAsTimestamp = Boolean.parseBoolean(options.getOrDefault("dateAsTimestamp", Boolean.toString(false)));
        Properties overriddenProps = extractPhoenixHBaseConfFromOptions(options);
        try (Connection conn = DriverManager.getConnection(
                JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + zkUrl, overriddenProps)) {
            List<ColumnInfo> columnInfos = PhoenixRuntime.generateColumnInfo(conn, tableName, null);
            Seq<ColumnInfo> columnInfoSeq = JavaConverters.asScalaIteratorConverter(columnInfos.iterator()).asScala().toSeq();
            schema = SparkSchemaUtil.phoenixSchemaToCatalystSchema(columnInfoSeq, dateAsTimestamp);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return schema;
    }

    @Override
    public Table getTable(StructType schema, Transform[] transforms, Map<String,String> properties) {
        return new PhoenixTestingTable(schema, properties);
    }

    @Override
    public String shortName() {
        return "phoenixTesting";
    }
}
