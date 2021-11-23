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
package org.apache.phoenix.spark.datasource.v2.writer;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.CURRENT_SCN_VALUE;
import static org.apache.phoenix.spark.datasource.v2.PhoenixDataSource.SKIP_NORMALIZING_IDENTIFIER;
import static org.apache.phoenix.spark.datasource.v2.PhoenixDataSource.ZOOKEEPER_URL;
import static org.apache.phoenix.spark.datasource.v2.PhoenixDataSource.extractPhoenixHBaseConfFromOptions;

public class PhoenixDataSourceWriter implements DataSourceWriter {

    private final PhoenixDataSourceWriteOptions options;

    public PhoenixDataSourceWriter(SaveMode mode, StructType schema, DataSourceOptions options) {
        if (!mode.equals(SaveMode.Overwrite)) {
            throw new RuntimeException("SaveMode other than SaveMode.OverWrite is not supported");
        }
        if (!options.tableName().isPresent()) {
            throw new RuntimeException("No Phoenix option " + DataSourceOptions.TABLE_KEY + " defined");
        }
        if (!options.get(ZOOKEEPER_URL).isPresent()) {
            throw new RuntimeException("No Phoenix option " + ZOOKEEPER_URL + " defined");
        }
        this.options = createPhoenixDataSourceWriteOptions(options, schema);
    }

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return new PhoenixDataWriterFactory(options);
    }

    @Override
    public boolean useCommitCoordinator() {
        return false;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
    }

    PhoenixDataSourceWriteOptions getOptions() {
        return options;
    }

    private PhoenixDataSourceWriteOptions createPhoenixDataSourceWriteOptions(DataSourceOptions options,
                                                                              StructType schema) {
        String scn = options.get(CURRENT_SCN_VALUE).orElse(null);
        String tenantId = options.get(PhoenixRuntime.TENANT_ID_ATTRIB).orElse(null);
        String zkUrl = options.get(ZOOKEEPER_URL).get();
        boolean skipNormalizingIdentifier = options.getBoolean(SKIP_NORMALIZING_IDENTIFIER, false);
        return new PhoenixDataSourceWriteOptions.Builder()
                .setTableName(options.tableName().get())
                .setZkUrl(zkUrl)
                .setScn(scn)
                .setTenantId(tenantId)
                .setSchema(schema)
                .setSkipNormalizingIdentifier(skipNormalizingIdentifier)
                .setOverriddenProps(extractPhoenixHBaseConfFromOptions(options))
                .build();
    }
}
