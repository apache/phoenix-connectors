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

package org.apache.hadoop.hive.ql;

//import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * QTestUtil. Cloned from Hive 3.0.0 as hive doesn't release hive-it-util artifact
 *
        */
    public class QTestUtil extends QTestUtilBase{


    public QTestUtil(String outDir, String logDir, MiniClusterType clusterType,
                     String confDir, String hadoopVer, String initScript, String cleanupScript,
                     boolean withLlapIo) throws Exception {
        super(outDir, logDir, clusterType, confDir, hadoopVer, initScript, cleanupScript,
                withLlapIo);
        DEFAULT_DATABASE_NAME = org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;
    }

    public QTestUtil(String outDir, String logDir, MiniClusterType clusterType,
                         String confDir, String hadoopVer, String initScript, String cleanupScript,
                         boolean withLlapIo, FsType fsType) throws Exception {
        super(outDir, logDir, clusterType, confDir, hadoopVer, initScript, cleanupScript,
                withLlapIo, fsType);
        DEFAULT_DATABASE_NAME = org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;
    }

    public MyResult doSetup(String confDir) throws MalformedURLException {
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
        conf.set(ConfVars.METASTORECONNECTURLKEY.varname, metaStoreURL);
        System.setProperty(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, metaStoreURL);
        return new MyResult(conf, new QueryState(conf));
    }




//  public void shutdown() throws Exception {
//    if (System.getenv(QTEST_LEAVE_FILES) == null) {
//      cleanUp();
//    }
//
//    setup.tearDown();
//    if (sparkSession != null) {
//      try {
//        SparkSessionManagerImpl.getInstance().closeSession(sparkSession);
//      } catch (Exception ex) {
//        LOG.error("Error closing spark session.", ex);
//      } finally {
//        sparkSession = null;
//      }
//    }
//    if (mr != null) {
//      mr.shutdown();
//      mr = null;
//    }
//    FileSystem.closeAll();
//    if (dfs != null) {
//      dfs.shutdown();
//      dfs = null;
//    }
//    Hive.closeCurrent();
//  }

    @Override
    public void init() throws Exception {

        // Create remote dirs once.
        if (mr != null) {
            createRemoteDirs();
        }

        // Create views registry

        testWarehouse = conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
        String execEngine = conf.get("hive.execution.engine");
        conf.set("hive.execution.engine", "mr");
        SessionState.start(conf);
        conf.set("hive.execution.engine", execEngine);
        db = Hive.get(conf);
        HiveMaterializedViewsRegistry.get().init(db);
        pd = new ParseDriver();
        sem = new SemanticAnalyzer(queryState);
    }

    public void convertSequenceFileToTextFile() throws Exception {
        // Create an instance of hive in order to create the tables
        testWarehouse = conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
        db = Hive.get(conf);

        // Drop dest4_sequencefile
        db.dropTable(DEFAULT_DATABASE_NAME, "dest4_sequencefile",
                true, true);
    }

    /**
     * Clear out any side effects of running tests
     */
    public void clearTestSideEffects() throws Exception {
        if (System.getenv(QTEST_LEAVE_FILES) != null) {
            return;
        }

        // allocate and initialize a new conf since a test can
        // modify conf by using 'set' commands
        conf = new HiveConf();
        initConf();
        initConfFromSetup();

        // renew the metastore since the cluster type is unencrypted
        db = Hive.get(conf);  // propagate new conf to meta store

        clearTablesCreatedDuringTests();
        clearUDFsCreatedDuringTests();
        clearKeysCreatedInTests();
    }


}
