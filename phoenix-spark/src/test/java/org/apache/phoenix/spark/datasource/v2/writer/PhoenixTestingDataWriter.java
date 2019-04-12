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

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import java.sql.SQLException;

public class PhoenixTestingDataWriter extends PhoenixDataWriter {

    private long numBatchesCommitted = 0;

    PhoenixTestingDataWriter(PhoenixDataSourceWriteOptions options) {
        super(options);
    }

    // Override to also count the number of times we call this method to test upsert batch commits
    @Override
    void commitBatchUpdates() throws SQLException {
        super.commitBatchUpdates();
        numBatchesCommitted++;
    }

    // Override to return a test WriterCommitMessage
    @Override
    public WriterCommitMessage commit() {
        super.commit();
        return new PhoenixTestingWriterCommitMessage(numBatchesCommitted);
    }

}
