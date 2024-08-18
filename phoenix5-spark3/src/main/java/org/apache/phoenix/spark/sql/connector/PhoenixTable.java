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

import static org.apache.spark.sql.connector.catalog.TableCapability.ACCEPT_ANY_SCHEMA;
import static org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ;
import static org.apache.spark.sql.connector.catalog.TableCapability.BATCH_WRITE;

import java.util.Map;
import java.util.Set;

import org.apache.phoenix.spark.sql.connector.reader.PhoenixScanBuilder;
import org.apache.phoenix.spark.sql.connector.writer.PhoenixWriteBuilder;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;


public class PhoenixTable implements SupportsRead, SupportsWrite {

    private final Map<String, String> options;
    private final String tableName;
    private final StructType schema;
    private static final Set<TableCapability> CAPABILITIES =
            ImmutableSet.of(BATCH_READ, BATCH_WRITE, ACCEPT_ANY_SCHEMA);

    public PhoenixTable(StructType schema, Map<String, String> options) {
        this.options = options;
        this.tableName = options.get(PhoenixDataSource.TABLE);
        this.schema = schema;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new PhoenixScanBuilder(schema, options);
    }

    @Override
    public String name() {
        return tableName;
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return CAPABILITIES;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new PhoenixWriteBuilder(info, options);
    }

}
