/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive.util;

        import org.apache.hadoop.hive.ql.io.AcidOutputFormat.Options;

/**
 * Misc utils for PhoenixStorageHandler
 */

public class PhoenixStorageHandlerUtil extends PhoenixStorageHandlerUtilBase {
    public static String getOptionsValue(Options options) {
        StringBuilder content = new StringBuilder();

        int bucket = options.getBucket();
        String inspectorInfo = options.getInspector().getCategory() + ":" + options.getInspector()
                .getTypeName();
        long maxTxnId = options.getMaximumTransactionId();
        long minTxnId = options.getMinimumTransactionId();
        int recordIdColumn = options.getRecordIdColumn();
        boolean isCompresses = options.isCompressed();
        boolean isWritingBase = options.isWritingBase();

        content.append("bucket : ").append(bucket).append(", inspectorInfo : ").append
                (inspectorInfo).append(", minTxnId : ").append(minTxnId).append(", maxTxnId : ")
                .append(maxTxnId).append(", recordIdColumn : ").append(recordIdColumn);
        content.append(", isCompressed : ").append(isCompresses).append(", isWritingBase : ")
                .append(isWritingBase);

        return content.toString();
    }
}
