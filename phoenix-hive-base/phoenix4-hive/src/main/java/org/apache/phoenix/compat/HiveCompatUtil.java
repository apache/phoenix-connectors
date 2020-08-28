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

import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.atomic.AtomicReference;

public class HiveCompatUtil {

    private HiveCompatUtil() {
        // Not to be instantiated
    }

    public static ExprNodeGenericFuncDesc getComparisonExpr(ExprNodeGenericFuncDesc comparisonExpr, boolean isNot) {
        return comparisonExpr;
    }

    public static String getOptionsValue(AcidOutputFormat.Options options, AtomicReference<Method> GET_BUCKET_METHOD_REF, AtomicReference<Method> GET_BUCKET_ID_METHOD_REF, Log LOG) {
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

    public static Object getDateOrTimestampValue(Object value){
        return null;
    }

    public static String getDefaultDatabaseName(){
        return org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;
    }

    public static MyResult doSetup(String confDir) throws MalformedURLException {
        // HIVE-14443 move this fall-back logic to CliConfigs
        if (confDir != null && !confDir.isEmpty()) {
            HiveConf.setHiveSiteLocation(new URL("file://"+ new File(confDir).toURI().getPath() + "/hive-site.xml"));
            System.out.println("Setting hive-site: "+ HiveConf.getHiveSiteLocation());
        }
        HiveConf conf = new HiveConf();
        String tmpBaseDir = System.getProperty("test.tmp.dir");
        if (tmpBaseDir == null || tmpBaseDir == "") {
            tmpBaseDir = System.getProperty("java.io.tmpdir");
        }
        String metaStoreURL = "jdbc:derby:" + tmpBaseDir + File.separator + "metastore_dbtest;" +
                "create=true";
        conf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, metaStoreURL);
        System.setProperty(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, metaStoreURL);
        return new MyResult(conf, new QueryState(conf));
    }

    public static void destroyTEZSession(SessionState sessionState){
    }

    public static Object getDriver(HiveConf conf){
        return null;
    }

    public static void cleanupQueryResultCache() { }

    public static HiveConf getHiveConf(){
        return new HiveConf();
    }

    public static int getDriverResponseCode(Object drv, String createTableCmd){
        return -1; // There is no driver in Phoenix4Hive2
    }

    public static void closeDriver(Object drv) { }

    public static QueryState getQueryState(HiveConf conf){
        return new QueryState(conf);
    }

    public static void initHiveMaterializedViewsRegistry() {}

    public static void initHiveMaterializedViewsRegistry(Hive db)
    {
        HiveMaterializedViewsRegistry.get().init(db);
    }
}
