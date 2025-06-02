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

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsOverwrite;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * The PhoenixWriteBuilder class is responsible for constructing
 * and configuring a write operation for Phoenix when interfacing
 * with Spark's data source API.
 * This class implements the WriteBuilder interface for write operations
 * and SupportsOverwrite interface to handle overwrite behavior.
 * The class facilitates the creation of a batch write operation
 * that is configured with the provided logical writing information
 * and options specific to the Phoenix data source.
 * Note: Overwrite mode does not do truncate table
 * and behaves the same as Append mode.
 */
public class PhoenixWriteBuilder implements WriteBuilder, SupportsOverwrite {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixWriteBuilder.class);

    private final LogicalWriteInfo writeInfo;
    private final Map<String,String> options;

    public PhoenixWriteBuilder(LogicalWriteInfo writeInfo, Map<String,String> options) {
        this.writeInfo = writeInfo;
        this.options = options;
    }

    @Override
    public BatchWrite buildForBatch() {
        return new PhoenixBatchWrite(writeInfo, options);
    }

    @VisibleForTesting
    LogicalWriteInfo getWriteInfo() {
        return writeInfo;
    }

    @VisibleForTesting
    Map<String,String> getOptions() {
        return options;
    }

    @Override
    public WriteBuilder overwrite(Filter[] filters) {
        LOGGER.info("Overwrite mode specified. Ignoring Overwrite and treating it as Append.");
        return this;
    }

}
