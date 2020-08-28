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
package org.apache.phoenix.compat;

import org.slf4j.Logger;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

public class HiveCompatUtil {

    private HiveCompatUtil() {
        // Not to be instantiated
    }

    public static ExprNodeGenericFuncDesc getComparisonExpr(ExprNodeGenericFuncDesc comparisonExpr, boolean isNot){
        ExprNodeGenericFuncDesc ret = comparisonExpr;
        try {
            if (GenericUDFIn.class == comparisonExpr.getGenericUDF().getClass() && isNot) {
                ret = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
                        FunctionRegistry.getFunctionInfo("not").getGenericUDF(),
                        Collections.singletonList(comparisonExpr));
            }
        } catch (SemanticException e) {
            throw new RuntimeException("hive operator -- never be thrown", e);
        }
        return ret;
    }

    public static String getOptionsValue(AcidOutputFormat.Options options, AtomicReference<Method> GET_BUCKET_METHOD_REF, AtomicReference<Method> GET_BUCKET_ID_METHOD_REF, Logger LOG) {
        StringBuilder content = new StringBuilder();

        int bucket = getBucket(options, GET_BUCKET_METHOD_REF, GET_BUCKET_ID_METHOD_REF, LOG);
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

    private static int getBucket(AcidOutputFormat.Options options, AtomicReference<Method> GET_BUCKET_METHOD_REF, AtomicReference<Method> GET_BUCKET_ID_METHOD_REF, Logger LOG) {
        Method getBucketMethod = GET_BUCKET_METHOD_REF.get();
        try {
            if (getBucketMethod == null) {
                getBucketMethod = AcidOutputFormat.Options.class.getMethod("getBucket");
                GET_BUCKET_METHOD_REF.set(getBucketMethod);
            }
            return (int) getBucketMethod.invoke(options);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException e) {
            LOG.trace("Failed to invoke Options.getBucket()", e);
        }
        Method getBucketIdMethod = GET_BUCKET_ID_METHOD_REF.get();
        try {
            if (getBucketIdMethod == null) {
                getBucketIdMethod = AcidOutputFormat.Options.class.getMethod("getBucketId");
                GET_BUCKET_ID_METHOD_REF.set(getBucketMethod);
            }
            return (int) getBucketIdMethod.invoke(options);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Failed to invoke Options.getBucketId()", e);
        }
    }

    public static Object getDateOrTimestampValue(Object value){
        if (value instanceof Date) {
            value = java.sql.Date.valueOf(value.toString());
        } else if (value instanceof Timestamp) {
            value = java.sql.Timestamp.valueOf(value.toString());
        }
        return value;
    }

    public static String getDefaultDatabaseName(){
        return org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
    }

    public static MyResult doSetup(String confDir) throws MalformedURLException {
        // HIVE-14443 move this fall-back logic to CliConfigs
        if (confDir != null && !confDir.isEmpty()) {
            HiveConf.setHiveSiteLocation(new URL("file://"+ new File(confDir).toURI().getPath() + "/hive-site.xml"));
            MetastoreConf.setHiveSiteLocation(HiveConf.getHiveSiteLocation());
            System.out.println("Setting hive-site: "+ HiveConf.getHiveSiteLocation());
        }

        QueryState queryState = new QueryState.Builder().withHiveConf(new HiveConf(IDriver.class)).build();
        HiveConf conf = queryState.getConf();
        return new MyResult(conf, queryState);
    }

    public static void destroyTEZSession(SessionState sessionState) throws Exception {
        sessionState.getTezSession().destroy();
    }

    public static Object getDriver(HiveConf conf){
        return DriverFactory.newDriver(conf);
    }

    public static void cleanupQueryResultCache(){
        // Remove any cached results from the previous test.
        QueryResultsCache.cleanupInstance();
    }

    public static HiveConf getHiveConf(){
        return new HiveConf(IDriver.class);
    }

    public static int getDriverResponseCode(Object drv, String createTableCmd){
        IDriver driver = (IDriver) drv;
        return driver.run(createTableCmd).getResponseCode();
    }

    public static void closeDriver(Object drv){
        IDriver driver = (IDriver) drv;
        driver.close();
    }

    public static QueryState getQueryState(HiveConf conf){
        return new QueryState.Builder().withHiveConf(conf).build();
    }

    public static void initHiveMaterializedViewsRegistry()
    {
        HiveMaterializedViewsRegistry.get().init();
    }

    public static void initHiveMaterializedViewsRegistry(Hive db) { }
}
