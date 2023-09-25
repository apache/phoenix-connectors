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
package org.apache.phoenix.spark.sql.connector.writer;

import org.apache.phoenix.spark.sql.connector.PhoenixDataSource;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.CURRENT_SCN_VALUE;
import static org.apache.phoenix.spark.sql.connector.PhoenixDataSource.SKIP_NORMALIZING_IDENTIFIER;
import static org.apache.phoenix.spark.sql.connector.PhoenixDataSource.extractPhoenixHBaseConfFromOptions;

public class PhoenixBatchWrite implements BatchWrite {

    private final PhoenixDataSourceWriteOptions options;
    private final LogicalWriteInfo writeInfo;

    PhoenixBatchWrite(LogicalWriteInfo writeInfo, Map<String,String> options) {
        this.writeInfo = writeInfo;
        this.options = createPhoenixDataSourceWriteOptions(options, writeInfo.schema());
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
        return new PhoenixDataWriterFactory(writeInfo.schema(),options);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
    }

    private PhoenixDataSourceWriteOptions createPhoenixDataSourceWriteOptions(Map<String,String> options,
                                                                                         StructType schema) {
        String scn = options.get(CURRENT_SCN_VALUE);
        String tenantId = options.get(PhoenixRuntime.TENANT_ID_ATTRIB);
        String jdbcUrl = PhoenixDataSource.getJdbcUrlFromOptions(options);
        String tableName = options.get("table");
        boolean skipNormalizingIdentifier = Boolean.parseBoolean(options.getOrDefault(SKIP_NORMALIZING_IDENTIFIER, Boolean.toString(false)));
        return new PhoenixDataSourceWriteOptions.Builder()
                .setTableName(tableName)
                .setJdbcUrl(jdbcUrl)
                .setScn(scn)
                .setTenantId(tenantId)
                .setSchema(schema)
                .setSkipNormalizingIdentifier(skipNormalizingIdentifier)
                .setOverriddenProps(extractPhoenixHBaseConfFromOptions(options))
                .build();
    }

    @VisibleForTesting
    LogicalWriteInfo getWriteInfo(){
        return writeInfo;
    }

    @VisibleForTesting
    PhoenixDataSourceWriteOptions getOptions() {
        return options;
    }
}
