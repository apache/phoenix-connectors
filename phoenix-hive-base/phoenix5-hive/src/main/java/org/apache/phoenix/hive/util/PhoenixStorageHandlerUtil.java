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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ConcurrentHashMap;
import javax.naming.NamingException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat.Options;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.net.DNS;
import org.apache.phoenix.compat.CompatUtil;
import org.apache.phoenix.hive.PrimaryKeyData;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.ql.index.IndexSearchCondition;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;

/**
 * Misc utils for PhoenixStorageHandler
 */

public class PhoenixStorageHandlerUtil extends PhoenixStorageHandlerUtilBase{
    public static String getOptionsValue(Options options) {
        StringBuilder content = new StringBuilder();

        int bucket = getBucket(options);
        String inspectorInfo = options.getInspector().getCategory() + ":" + options.getInspector()
                .getTypeName();
        long maxTxnId = options.getMaximumWriteId();
        long minTxnId = options.getMinimumWriteId();
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

    private static int getBucket(Options options) {
        Method getBucketMethod = GET_BUCKET_METHOD_REF.get();
        try {
            if (getBucketMethod == null) {
                getBucketMethod = Options.class.getMethod("getBucket");
                GET_BUCKET_METHOD_REF.set(getBucketMethod);
            }
            return (int) getBucketMethod.invoke(options);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException e) {
            LOG.trace("Failed to invoke Options.getBucket()", e);
        }
        Method getBucketIdMethod = GET_BUCKET_ID_METHOD_REF.get();
        try {
            if (getBucketIdMethod == null) {
                getBucketIdMethod = Options.class.getMethod("getBucketId");
                GET_BUCKET_ID_METHOD_REF.set(getBucketMethod);
            }
            return (int) getBucketIdMethod.invoke(options);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Failed to invoke Options.getBucketId()", e);
        }
    }
}
