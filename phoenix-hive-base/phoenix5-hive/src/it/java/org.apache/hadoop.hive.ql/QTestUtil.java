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


import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.EnumSet;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.logging.log4j.util.Strings;



/**
 * QTestUtil. Cloned from Hive 3.0.0 as hive doesn't release hive-it-util artifact
 *
 */
public class QTestUtil extends QTestUtilBase {
    private IDriver drv;

    public QTestUtil(String outDir, String logDir, MiniClusterType clusterType,
                     String confDir, String hadoopVer, String initScript, String cleanupScript,
                     boolean withLlapIo) throws Exception {
        super(outDir, logDir, clusterType, confDir, hadoopVer, initScript, cleanupScript,
                withLlapIo);
        DEFAULT_DATABASE_NAME = org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
    }

    public QTestUtil(String outDir, String logDir, MiniClusterType clusterType,
                     String confDir, String hadoopVer, String initScript, String cleanupScript,
                     boolean withLlapIo, FsType fsType) throws Exception {
        super(outDir, logDir, clusterType, confDir, hadoopVer, initScript, cleanupScript,
                withLlapIo, fsType);
        DEFAULT_DATABASE_NAME = org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
    }


    public MyResult doSetup(String confDir) throws MalformedURLException {
        // HIVE-14443 move this fall-back logic to CliConfigs
        if (confDir != null && !confDir.isEmpty()) {
            HiveConf.setHiveSiteLocation(new URL("file://"+ new File(confDir).toURI().getPath() + "/hive-site.xml"));
            MetastoreConf.setHiveSiteLocation(HiveConf.getHiveSiteLocation());
            System.out.println("Setting hive-site: "+ HiveConf.getHiveSiteLocation());
        }

        queryState = new QueryState.Builder().withHiveConf(new HiveConf(IDriver.class)).build();
        conf = queryState.getConf();
        return new MyResult(conf, queryState);
    }

    private void setupFileSystem(HadoopShims shims) throws IOException {

        if (fsType == FsType.local) {
            fs = FileSystem.getLocal(conf);
        } else if (fsType == FsType.hdfs || fsType == FsType.encrypted_hdfs) {
            int numDataNodes = 4;

            if (fsType == FsType.encrypted_hdfs) {
                // Set the security key provider so that the MiniDFS cluster is initialized
                // with encryption
                conf.set(SECURITY_KEY_PROVIDER_URI_NAME, getKeyProviderURI());
                conf.setInt("fs.trash.interval", 50);

                dfs = shims.getMiniDfs(conf, numDataNodes, true, null);
                fs = dfs.getFileSystem();

                // set up the java key provider for encrypted hdfs cluster
                hes = shims.createHdfsEncryptionShim(fs, conf);

                LOG.info("key provider is initialized");
            } else {
                dfs = shims.getMiniDfs(conf, numDataNodes, true, null);
                fs = dfs.getFileSystem();
            }
        } else {
            throw new IllegalArgumentException("Unknown or unhandled fsType [" + fsType + "]");
        }
    }

    private void setupMiniCluster(HadoopShims shims, String confDir) throws
            IOException {

        String uriString = fs.getUri().toString();

        if (clusterType.getCoreClusterType() == CoreClusterType.TEZ) {
            if (confDir != null && !confDir.isEmpty()) {
                conf.addResource(new URL("file://" + new File(confDir).toURI().getPath()
                        + "/tez-site.xml"));
            }
            int numTrackers = 2;
            if (EnumSet.of(MiniClusterType.llap_local, MiniClusterType.tez_local).contains(clusterType)) {
                mr = shims.getLocalMiniTezCluster(conf, clusterType == MiniClusterType.llap_local);
            } else {
                mr = shims.getMiniTezCluster(conf, numTrackers, uriString,
                        EnumSet.of(MiniClusterType.llap, MiniClusterType.llap_local).contains(clusterType));
            }
        } else if (clusterType == MiniClusterType.miniSparkOnYarn) {
            mr = shims.getMiniSparkCluster(conf, 2, uriString, 1);
        } else if (clusterType == MiniClusterType.mr) {
            mr = shims.getMiniMrCluster(conf, 2, uriString, 1);
        }
    }

    public void shutdown() throws Exception {
        if (System.getenv(QTEST_LEAVE_FILES) == null) {
            cleanUp();
        }

        if (clusterType.getCoreClusterType() == CoreClusterType.TEZ) {
            SessionState.get().getTezSession().destroy();
        }

        setup.tearDown();
        if (sparkSession != null) {
            try {
                SparkSessionManagerImpl.getInstance().closeSession(sparkSession);
            } catch (Exception ex) {
                LOG.error("Error closing spark session.", ex);
            } finally {
                sparkSession = null;
            }
        }
        if (mr != null) {
            mr.shutdown();
            mr = null;
        }
        FileSystem.closeAll();
        if (dfs != null) {
            dfs.shutdown();
            dfs = null;
        }
        Hive.closeCurrent();
    }

    /**
     * Clear out any side effects of running tests
     */
    public void clearTestSideEffects() throws Exception {
        if (System.getenv(QTEST_LEAVE_FILES) != null) {
            return;
        }

        // Remove any cached results from the previous test.
        QueryResultsCache.cleanupInstance();

        // allocate and initialize a new conf since a test can
        // modify conf by using 'set' commands
        conf = new HiveConf(IDriver.class);
        initConf();
        initConfFromSetup();

        // renew the metastore since the cluster type is unencrypted
        db = Hive.get(conf);  // propagate new conf to meta store

        clearTablesCreatedDuringTests();
        clearUDFsCreatedDuringTests();
        clearKeysCreatedInTests();
    }


    protected void runCreateTableCmd(String createTableCmd) throws Exception {
        int ecode = 0;
        ecode = drv.run(createTableCmd).getResponseCode();
        if (ecode != 0) {
            throw new Exception("create table command: " + createTableCmd
                    + " failed with exit code= " + ecode);
        }

        return;
    }

    protected void runCmd(String cmd) throws Exception {
        int ecode = 0;
        ecode = drv.run(cmd).getResponseCode();
        drv.close();
        if (ecode != 0) {
            throw new Exception("command: " + cmd
                    + " failed with exit code= " + ecode);
        }
        return;
    }

    public int execute(String tname) {
        return drv.run(qMap.get(tname)).getResponseCode();
    }

    public void convertSequenceFileToTextFile() throws Exception {
        // Create an instance of hive in order to create the tables
        testWarehouse = conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
        db = Hive.get(conf);

        // Move all data from dest4_sequencefile to dest4
        drv
                .run("FROM dest4_sequencefile INSERT OVERWRITE TABLE dest4 SELECT dest4_sequencefile.*");

        // Drop dest4_sequencefile
        db.dropTable(Warehouse.DEFAULT_DATABASE_NAME, "dest4_sequencefile",
                true, true);
    }

    public void resetParser() throws SemanticException {
        pd = new ParseDriver();
        queryState = new QueryState.Builder().withHiveConf(conf).build();
        sem = new SemanticAnalyzer(queryState);
    }

    /**
     * Setup to execute a set of query files. Uses QTestUtilBase to do so.
     *
     * @param qfiles
     *          array of input query files containing arbitrary number of hive
     *          queries
     * @param resDir
     *          output directory
     * @param logDir
     *          log directory
     * @return one QTestUtilBase for each query file
     */
    public static QTestUtil[] queryListRunnerSetup(File[] qfiles, String resDir,
                                                   String logDir, String initScript,
                                                   String cleanupScript) throws Exception
    {
        QTestUtil[] qt = new QTestUtil[qfiles.length];
        for (int i = 0; i < qfiles.length; i++) {
            qt[i] = new QTestUtil(resDir, logDir, MiniClusterType.none, null, "0.20",
                    initScript == null ? defaultInitScript : initScript,
                    cleanupScript == null ? defaultCleanupScript : cleanupScript, false);
            qt[i].addFile(qfiles[i]);
            qt[i].clearTestSideEffects();
        }

        return qt;
    }

    /**
     * Executes a set of query files in sequence.
     *
     * @param qfiles
     *          array of input query files containing arbitrary number of hive
     *          queries
     * @param qt
     *          array of QTestUtils, one per qfile
     * @return true if all queries passed, false otw
     */
    public static boolean queryListRunnerSingleThreaded(File[] qfiles, QTestUtil[] qt)
            throws Exception
    {
        boolean failed = false;
        qt[0].cleanUp();
        qt[0].createSources();
        for (int i = 0; i < qfiles.length && !failed; i++) {
            qt[i].clearTestSideEffects();
            qt[i].cliInit(qfiles[i].getName(), false);
            qt[i].executeClient(qfiles[i].getName());
            QTestProcessExecResult result = qt[i].checkCliDriverResults(qfiles[i].getName());
            if (result.getReturnCode() != 0) {
                failed = true;
                StringBuilder builder = new StringBuilder();
                builder.append("Test ")
                        .append(qfiles[i].getName())
                        .append(" results check failed with error code ")
                        .append(result.getReturnCode());
                if (Strings.isNotEmpty(result.getCapturedOutput())) {
                    builder.append(" and diff value ").append(result.getCapturedOutput());
                }
                System.err.println(builder.toString());
                outputTestFailureHelpMessage();
            }
            qt[i].clearPostTestEffects();
        }
        return (!failed);
    }



    /**
     * Executes a set of query files parallel.
     *
     * Each query file is run in a separate thread. The caller has to arrange
     * that different query files do not collide (in terms of destination tables)
     *
     * @param qfiles
     *          array of input query files containing arbitrary number of hive
     *          queries
     * @param qt
     *          array of QTestUtils, one per qfile
     * @return true if all queries passed, false otw
     *
     */
    public static boolean queryListRunnerMultiThreaded(File[] qfiles, QTestUtil[] qt)
            throws Exception
    {
        boolean failed = false;

        // in multithreaded mode - do cleanup/initialization just once

        qt[0].cleanUp();
        qt[0].createSources();
        qt[0].clearTestSideEffects();

        QTRunner[] qtRunners = new QTRunner[qfiles.length];
        Thread[] qtThread = new Thread[qfiles.length];

        for (int i = 0; i < qfiles.length; i++) {
            qtRunners[i] = new QTRunner(qt[i], qfiles[i].getName());
            qtThread[i] = new Thread(qtRunners[i]);
        }

        for (int i = 0; i < qfiles.length; i++) {
            qtThread[i].start();
        }

        for (int i = 0; i < qfiles.length; i++) {
            qtThread[i].join();
            QTestProcessExecResult result = qt[i].checkCliDriverResults(qfiles[i].getName());
            if (result.getReturnCode() != 0) {
                failed = true;
                StringBuilder builder = new StringBuilder();
                builder.append("Test ")
                        .append(qfiles[i].getName())
                        .append(" results check failed with error code ")
                        .append(result.getReturnCode());
                if (Strings.isNotEmpty(result.getCapturedOutput())) {
                    builder.append(" and diff value ").append(result.getCapturedOutput());
                }
                System.err.println(builder.toString());
                outputTestFailureHelpMessage();
            }
        }
        return (!failed);
    }

    @Override
    public void init() throws Exception {

        // Create remote dirs once.
        if (mr != null) {
            createRemoteDirs();
        }

        // Create views registry
        HiveMaterializedViewsRegistry.get().init();

        testWarehouse = conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
        String execEngine = conf.get("hive.execution.engine");
        conf.set("hive.execution.engine", "mr");
        SessionState.start(conf);
        conf.set("hive.execution.engine", execEngine);
        db = Hive.get(conf);
        drv = DriverFactory.newDriver(conf);
        pd = new ParseDriver();
        sem = new SemanticAnalyzer(queryState);
    }

}
