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
package org.apache.phoenix.hive.mapreduce;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.phoenix.compat.CompatUtil;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.ppd.PhoenixPredicateDecomposer;
import org.apache.phoenix.hive.ql.index.IndexSearchCondition;
import org.apache.phoenix.hive.query.PhoenixQueryBuilder;
import org.apache.phoenix.hive.util.PhoenixConnectionUtil;
import org.apache.phoenix.hive.util.PhoenixStorageHandlerUtil;
import org.apache.phoenix.iterate.MapReduceParallelScanGrouper;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom InputFormat to feed into Hive
 */
@SuppressWarnings({"deprecation", "rawtypes"})
public class PhoenixInputFormat<T extends DBWritable> implements InputFormat<WritableComparable,
        T> {

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixInputFormat.class);
    public PhoenixInputFormat() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("PhoenixInputFormat created");
        }
    }

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        String tableName = jobConf.get(PhoenixStorageHandlerConstants.PHOENIX_TABLE_NAME);

        String query;
        String executionEngine = jobConf.get(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname,
                HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.getDefaultValue());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Target table name at split phase : " + tableName + "with whereCondition :" +
                    jobConf.get(TableScanDesc.FILTER_TEXT_CONF_STR) +
                    " and " + HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname + " : " +
                    executionEngine);
        }

        List<IndexSearchCondition> conditionList = null;
        String filterExprSerialized = jobConf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
        if (filterExprSerialized != null) {
            ExprNodeGenericFuncDesc filterExpr =
                    SerializationUtilities.deserializeExpression(filterExprSerialized);
            PhoenixPredicateDecomposer predicateDecomposer =
                    PhoenixPredicateDecomposer
                      .create(Arrays.asList(jobConf.get(serdeConstants.LIST_COLUMNS).split(",")));
            predicateDecomposer.decomposePredicate(filterExpr);
            if (predicateDecomposer.isCalledPPD()) {
                conditionList = predicateDecomposer.getSearchConditionList();
            }
        }

        query = PhoenixQueryBuilder.getInstance().buildQuery(jobConf, tableName,
                PhoenixStorageHandlerUtil.getReadColumnNames(jobConf), conditionList);

        final QueryPlan queryPlan = getQueryPlan(jobConf, query);
        final List<KeyRange> allSplits = queryPlan.getSplits();
        final List<InputSplit> splits = generateSplits(jobConf, queryPlan, allSplits, query);

        return splits.toArray(new InputSplit[splits.size()]);
    }

    private List<InputSplit> generateSplits(final JobConf jobConf, final QueryPlan qplan,
                                            final List<KeyRange> splits, final String query)
            throws IOException {

        if (qplan == null) {
            throw new NullPointerException();
        }
        if (splits == null) {
            throw new NullPointerException();
        }
        final List<InputSplit> psplits = new ArrayList<>(splits.size());

        final Path[] tablePaths = FileInputFormat.getInputPaths(
                ShimLoader.getHadoopShims().newJobContext(new Job(jobConf)));
        final boolean splitByStats = jobConf.getBoolean(
                PhoenixStorageHandlerConstants.SPLIT_BY_STATS,
                false);
        final int parallelThreshold = jobConf.getInt(
                PhoenixStorageHandlerConstants.PHOENIX_MINIMUM_PARALLEL_SCANS_THRESHOLD,
                PhoenixStorageHandlerConstants.DEFAULT_PHOENIX_MINIMUM_PARALLEL_SCANS_THRESHOLD);
        setScanCacheSize(jobConf);
        try (org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory
                .createConnection(PhoenixConnectionUtil.getConfiguration(jobConf))) {
            final RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(
                            qplan.getTableRef().getTable().getPhysicalName().toString()));
            final int scanSize = qplan.getScans().size();
            if (useParallelInputGeneration(parallelThreshold, scanSize)) {
                final int parallelism = jobConf.getInt(
                        PhoenixStorageHandlerConstants.PHOENIX_INPUTSPLIT_GENERATION_THREAD_COUNT,
                        PhoenixStorageHandlerConstants
                                .DEFAULT_PHOENIX_INPUTSPLIT_GENERATION_THREAD_COUNT);
                ExecutorService executorService = Executors.newFixedThreadPool(parallelism);
                LOG.info("Generating Input Splits in Parallel with {} threads", parallelism);
                List<Future<List<InputSplit>>> tasks = new ArrayList<>();

                try {
                    for (final List<Scan> scans : qplan.getScans()) {
                        Future<List<InputSplit>> task = executorService.submit(
                                new Callable<List<InputSplit>>() {
                                    @Override public List<InputSplit> call() throws Exception {
                                        return generateSplitsInternal(query, scans, splitByStats,
                                                connection, regionLocator, tablePaths);
                                    }
                                });
                        tasks.add(task);
                    }
                    for (Future<List<InputSplit>> task : tasks) {
                        psplits.addAll(task.get());
                    }
                } catch (ExecutionException|InterruptedException e) {
                    Throwable throwable = e.getCause();
                    if (throwable instanceof IOException) {
                        throw (IOException) throwable;
                    } else {
                        throw new IOException(e);
                    }
                } finally {
                    executorService.shutdown();
                }
            } else {
                LOG.info("Generating Input Splits in Serial");
                for (final List<Scan> scans : qplan.getScans()) {
                    psplits.addAll(generateSplitsInternal(query, scans,
                            splitByStats, connection, regionLocator, tablePaths));
                }
            }
        }

        return psplits;
    }

    /**
     * This method is used to check whether need to run in parallel to reduce time costs.
     * @param parallelThreshold parameter parallelThreshold
     * @param scans number of scans
     * @return true indicates should generate split in parallel.
     */
    private boolean useParallelInputGeneration(final int parallelThreshold, final int scans) {
        return parallelThreshold > 0 && scans >= parallelThreshold;
    }

    /**
     * This method is used to generate splits for each scan list.
     * @param query phoenix query statement
     * @param scans scan list slice of query plan
     * @param splitByStats split by stat enabled
     * @param connection phoenix connection
     * @param regionLocator Hbase Region Locator
     * @param tablePaths table paths
     * @return List of Input Splits
     * @throws IOException if function fails
     */
    private List<InputSplit> generateSplitsInternal(final String query, final List<Scan> scans,
            final boolean splitByStats, final org.apache.hadoop.hbase.client.Connection connection,
            final RegionLocator regionLocator, final Path[] tablePaths) throws IOException {

        final List<InputSplit> psplits = new ArrayList<>(scans.size());

        PhoenixInputSplit inputSplit;

        HRegionLocation location = regionLocator.getRegionLocation(scans.get(0).getStartRow(),
                false);
        long regionSize = CompatUtil.getSize(regionLocator, connection.getAdmin(), location);
        String regionLocation = PhoenixStorageHandlerUtil.getRegionLocation(location, LOG);

        if (splitByStats) {
            for (Scan aScan : scans) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Split for  scan : " + aScan + "with scanAttribute : "
                            + aScan.getAttributesMap() + " [scanCache, cacheBlock, scanBatch] : ["
                            + aScan.getCaching() + ", " + aScan.getCacheBlocks() + ", "
                            + aScan.getBatch() + "] and  regionLocation : " + regionLocation);
                }

                inputSplit = new PhoenixInputSplit(new ArrayList<>(Arrays.asList(aScan)),
                        tablePaths[0], regionLocation, regionSize);
                inputSplit.setQuery(query);
                psplits.add(inputSplit);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Scan count[" + scans.size() + "] : " + Bytes.toStringBinary(scans
                        .get(0).getStartRow()) + " ~ " + Bytes.toStringBinary(scans.get(scans
                        .size() - 1).getStopRow()));

                LOG.debug("First scan : " + scans.get(0) + "with scanAttribute : " + scans
                        .get(0).getAttributesMap() + " [scanCache, cacheBlock, scanBatch] : "
                        + "[" + scans.get(0).getCaching() + ", " + scans.get(0).getCacheBlocks()
                        + ", " + scans.get(0).getBatch() + "] and  regionLocation : "
                        + regionLocation);

                for (int i = 0, limit = scans.size(); i < limit; i++) {
                    LOG.debug("EXPECTED_UPPER_REGION_KEY[" + i + "] : " + Bytes
                                .toStringBinary(scans.get(i).getAttribute
                                        (BaseScannerRegionObserver.EXPECTED_UPPER_REGION_KEY)));
                }
            }

            inputSplit = new PhoenixInputSplit(scans, tablePaths[0], regionLocation, regionSize);
            inputSplit.setQuery(query);
            psplits.add(inputSplit);
        }

        return psplits;
    }

    private void setScanCacheSize(JobConf jobConf) {
        int scanCacheSize = jobConf.getInt(PhoenixStorageHandlerConstants.HBASE_SCAN_CACHE, -1);
        if (scanCacheSize > 0) {
            jobConf.setInt(HConstants.HBASE_CLIENT_SCANNER_CACHING, scanCacheSize);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Generating splits with scanCacheSize : " + scanCacheSize);
        }
    }

    @Override
    public RecordReader<WritableComparable, T> getRecordReader(InputSplit split, JobConf job,
                                                               Reporter reporter) throws
            IOException {
        final QueryPlan queryPlan = getQueryPlan(job, ((PhoenixInputSplit) split).getQuery());
        @SuppressWarnings("unchecked")
        final Class<T> inputClass = (Class<T>) job.getClass(PhoenixConfigurationUtil.INPUT_CLASS,
                PhoenixResultWritable.class);

        PhoenixRecordReader<T> recordReader = new PhoenixRecordReader<T>(inputClass, job,
                queryPlan);
        recordReader.initialize(split);

        return recordReader;
    }

    /**
     * Returns the query plan associated with the select query.
     */
    private QueryPlan getQueryPlan(final Configuration configuration, String selectStatement)
            throws IOException {
        try {
            final String currentScnValue = configuration.get(PhoenixConfigurationUtil
                    .CURRENT_SCN_VALUE);
            final Properties overridingProps = new Properties();
            if (currentScnValue != null) {
                overridingProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, currentScnValue);
            }
            final Connection connection = PhoenixConnectionUtil.getInputConnection(configuration,
                    overridingProps);
            if (selectStatement == null) {
                throw new NullPointerException();
            }
            final Statement statement = connection.createStatement();
            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Compiled query : " + selectStatement);
            }

            // Optimize the query plan so that we potentially use secondary indexes
            final QueryPlan queryPlan = pstmt.optimizeQuery(selectStatement);
            // Initialize the query plan so it sets up the parallel scans
            queryPlan.iterator(MapReduceParallelScanGrouper.getInstance());
            return queryPlan;
        } catch (Exception exception) {
            LOG.error(String.format("Failed to get the query plan with error [%s]", exception.getMessage()));
            throw new RuntimeException(exception);
        }
    }
}
