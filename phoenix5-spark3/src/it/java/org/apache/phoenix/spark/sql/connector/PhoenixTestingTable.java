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
package org.apache.phoenix.spark.sql.connector;

import org.apache.phoenix.spark.sql.connector.reader.PhoenixTestScanBuilder;
import org.apache.phoenix.spark.sql.connector.writer.PhoenixTestWriteBuilder;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;


public class PhoenixTestingTable extends PhoenixTable {

    PhoenixTestingTable(StructType schema, Map<String, String> options) {
        super(schema, options);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new PhoenixTestScanBuilder(schema(), options);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new PhoenixTestWriteBuilder(info,getOptions());
    }

}
