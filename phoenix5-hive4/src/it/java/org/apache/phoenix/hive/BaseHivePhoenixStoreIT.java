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
package org.apache.phoenix.hive;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.security.Policy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QTestArguments;
import org.apache.hadoop.hive.ql.QTestMiniClusters;
import org.apache.hadoop.hive.ql.QTestProcessExecResult;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hive.hcatalog.DerbyPolicy;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.base.Throwables;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.AfterClass;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all Hive Phoenix integration tests that may be run with Tez or MR mini cluster
 */
@NotThreadSafe
@Category(ParallelStatsDisabledTest.class)
public class BaseHivePhoenixStoreIT extends BaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(BaseHivePhoenixStoreIT.class);
    protected static QTestUtil qt;
    protected static String hiveOutputDir;
    protected static String hiveLogDir;

    // TODO This should be replaced with a copy of the HBase test environment setup code from Hive
    public static void setup(QTestMiniClusters.MiniClusterType clusterType) throws Exception {
        System.clearProperty("test.build.data");

        Policy.setPolicy(new DerbyPolicy());
        
        //Setup Hbase minicluster + Phoenix first
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(3);
        serverProps.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        serverProps.put("hive.metastore.schema.verification","false");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()));

        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        if (null != hadoopConfDir && !hadoopConfDir.isEmpty()) {
          LOG.warn("WARNING: HADOOP_CONF_DIR is set in the environment which may cause "
              + "issues with test execution via MiniDFSCluster");
        }

        // Setup Hive mini Server
        hiveOutputDir = new Path(utility.getDataTestDir(), "hive_output").toString();
        File outputDir = new File(hiveOutputDir);
        outputDir.mkdirs();

        hiveLogDir = new Path(utility.getDataTestDir(), "hive_log").toString();
        File logDir = new File(hiveLogDir);
        logDir.mkdirs();

        Path testRoot = utility.getDataTestDir();
        String hiveBuildDataDir = new Path(utility.getDataTestDir(), "hive/build/data/").toString();
        File buildDataDir = new File(hiveBuildDataDir);
        buildDataDir.mkdirs();

        //Separate data dir for the Hive test cluster's DFS, so that it doesn't nuke HBase's DFS
        System.setProperty("test.build.data", hiveBuildDataDir.toString());

        System.setProperty("test.tmp.dir", testRoot.toString());
        // TODO set in testHiveConf ?
        System.setProperty(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION.toString(), "false");

        Map<HiveConf.ConfVars, String> testHiveConf = new HashMap<>();
        testHiveConf.put(HiveConf.ConfVars.METASTORE_WAREHOUSE,
            (new Path(testRoot, "warehouse")).toString());
        testHiveConf.put(HiveConf.ConfVars.HIVE_TESTING_REMOVE_LOGS,
            "false");
        testHiveConf.put(HiveConf.ConfVars.METASTORE_AUTO_CREATE_ALL,"true");
        testHiveConf.put(HiveConf.ConfVars.HIVE_CHECK_CROSS_PRODUCT,"false");

        try {
            QTestArguments qtArgs = QTestArguments.QTestArgumentsBuilder.instance()
                    .withOutDir(hiveOutputDir)
                    .withLogDir(hiveLogDir)
                    .withClusterType(clusterType)
                    .withCustomConfigValueMap(testHiveConf)
                    .build();
            // These were set for Hive3, but are not required for Hive4
            // conf.set("mapreduce.job.name", "test");
            // conf.set("hive.mapred.mode", "nonstrict");
            // conf.set("hive.strict.checks.cartesian.product", "false");
            qt = new QTestUtil(qtArgs);
        } catch (Exception e) {
            LOG.error("Unexpected exception in setup: " + e.getMessage(), e);
            fail("Unexpected exception in setup"+Throwables.getStackTraceAsString(e));
        }

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                Statement stmt = conn.createStatement()) {
            stmt.execute("create table t(a integer primary key,b varchar)");
        }

    }

    protected void runTest(String fname, String fpath) throws Exception {
        long startTime = System.currentTimeMillis();
        try {
            LOG.info("Begin query: " + fname);
            qt.clearTestSideEffects();
            qt.newSession();
            qt.setInputFile(fpath);
            qt.cliInit();

            CommandProcessorResponse resp = qt.executeClient();
            if (resp.getMessage() != null &&  !resp.getMessage().isEmpty()) {
                qt.failedQuery(null, -9999, fname, "Command has failed with response:" + resp.toString());
                return;
            }

            QTestProcessExecResult result = qt.checkCliDriverResults();
            if (result.getReturnCode() != 0) {
              qt.failedDiff(result.getReturnCode(), fname, result.getCapturedOutput());
            }
            qt.clearPostTestEffects();

        } catch (java.lang.AssertionError a) {
            throw a;
        } catch (Throwable e) {
            qt.failedQuery(new Exception(e), -9999, fname, "Command has thrown exception");
        }

        long elapsedTime = System.currentTimeMillis() - startTime;
        LOG.info("Done query: " + fname + " elapsedTime=" + elapsedTime / 1000 + "s");
        assertTrue("Test passed", true);
    }

    protected void createFile(String content, String fullName) throws IOException {
        FileUtils.write(new File(fullName), content);
    }

    @AfterClass
    public static synchronized void tearDownAfterClass() throws Exception {
        // Shutdowns down the filesystem -- do this after stopping HBase.
        if (qt != null) {
          try {
              qt.shutdown();
          } catch (Exception e) {
              LOG.error("Unexpected exception in setup", e);
              //fail("Unexpected exception in tearDown");
          }
      }
    }
}
