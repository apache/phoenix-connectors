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
package org.apache.phoenix.spark.datasource.v2.reader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.generated.PTableProtos.PTable;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.iterate.ConcatResultIterator;
import org.apache.phoenix.iterate.LookAheadResultIterator;
import org.apache.phoenix.iterate.MapReduceParallelScanGrouper;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.RoundRobinResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.iterate.TableResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.phoenix.monitoring.ReadMetricQueue;
import org.apache.phoenix.monitoring.ScanMetricsHolder;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.spark.SerializableWritable;
import org.apache.spark.executor.InputMetrics;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.SparkJdbcUtil;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;

import scala.collection.Iterator;

public class PhoenixInputPartitionReader implements InputPartitionReader<InternalRow>  {

    private final SerializableWritable<PhoenixInputSplit> phoenixInputSplit;
    private final StructType schema;
    private final PhoenixDataSourceReadOptions options;
    private Iterator<InternalRow> iterator;
    private PhoenixResultSet resultSet;
    private InternalRow currentRow;

    PhoenixInputPartitionReader(PhoenixDataSourceReadOptions options, StructType schema,
            SerializableWritable<PhoenixInputSplit> phoenixInputSplit) {
        this.options = options;
        this.phoenixInputSplit = phoenixInputSplit;
        this.schema = schema;
        initialize();
    }

    Properties getOverriddenPropsFromOptions() {
        return options.getOverriddenProps();
    }

    private QueryPlan getQueryPlan() throws SQLException {
        String scn = options.getScn();
        String tenantId = options.getTenantId();
        String jdbcUrl = options.getJdbcUrl();
        Properties overridingProps = getOverriddenPropsFromOptions();
        if (scn != null) {
            overridingProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, scn);
        }
        if (tenantId != null) {
            overridingProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        try (Connection conn = DriverManager.getConnection(jdbcUrl, overridingProps)) {
            PTable pTable = null;
            try {
                pTable = PTable.parseFrom(options.getPTableCacheBytes());
            } catch (Exception e) {
                throw new RuntimeException("Parsing the PTable Cache Bytes is failing ", e);
            }
            org.apache.phoenix.schema.PTable table = PTableImpl.createFromProto(pTable);
            PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);
            phoenixConnection.addTable(table, System.currentTimeMillis());
            final Statement statement = conn.createStatement();
            final String selectStatement = options.getSelectStatement();
            if (selectStatement == null){
                throw new NullPointerException();
            }

            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            // Optimize the query plan so that we potentially use secondary indexes
            return pstmt.optimizeQuery(selectStatement);
        }
    }

    private void initialize() {
        try {
            final QueryPlan queryPlan = getQueryPlan();
            final List<Scan> scans = phoenixInputSplit.value().getScans();
            List<PeekingResultIterator> iterators = new ArrayList<>(scans.size());
            StatementContext ctx = queryPlan.getContext();
            ReadMetricQueue readMetrics = ctx.getReadMetricsQueue();
            String tableName = queryPlan.getTableRef().getTable().getPhysicalName().getString();

            // Clear the table region boundary cache to make sure long running jobs stay up to date
            byte[] tableNameBytes = queryPlan.getTableRef().getTable().getPhysicalName().getBytes();
            ConnectionQueryServices services = queryPlan.getContext().getConnection().getQueryServices();
            services.clearTableRegionCache(TableName.valueOf(tableNameBytes));

            long renewScannerLeaseThreshold = queryPlan.getContext().getConnection()
                    .getQueryServices().getRenewLeaseThresholdMilliSeconds();
            for (Scan scan : scans) {
                // For MR, skip the region boundary check exception if we encounter a split. ref: PHOENIX-2599
                scan.setAttribute(BaseScannerRegionObserverConstants.SKIP_REGION_BOUNDARY_CHECK, Bytes.toBytes(true));

                PeekingResultIterator peekingResultIterator;
                ScanMetricsHolder scanMetricsHolder =
                        ScanMetricsHolder.getInstance(readMetrics, tableName, scan,
                                queryPlan.getContext().getConnection().getLogLevel());
                final TableResultIterator tableResultIterator =
                        new TableResultIterator(
                                queryPlan.getContext().getConnection().getMutationState(), scan,
                                scanMetricsHolder, renewScannerLeaseThreshold, queryPlan,
                                MapReduceParallelScanGrouper.getInstance());
                peekingResultIterator = LookAheadResultIterator.wrap(tableResultIterator);
                iterators.add(peekingResultIterator);
            }
            ResultIterator iterator = queryPlan.useRoundRobinIterator() ?
                    RoundRobinResultIterator.newIterator(iterators, queryPlan) :
                    ConcatResultIterator.newIterator(iterators);
            if (queryPlan.getContext().getSequenceManager().getSequenceCount() > 0) {
                iterator = new SequenceResultIterator(iterator, queryPlan.getContext().getSequenceManager());
            }
            // Clone the row projector as it's not thread safe and would be used simultaneously by
            // multiple threads otherwise.
            this.resultSet = new PhoenixResultSet(iterator, queryPlan.getProjector().cloneIfNecessary(),
                    queryPlan.getContext());
            this.iterator = SparkJdbcUtil.resultSetToSparkInternalRows(resultSet, schema, new InputMetrics());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean next() {
        if (!iterator.hasNext()) {
            return false;
        }
        currentRow = iterator.next();
        return true;
    }

    @Override
    public InternalRow get() {
        return currentRow;
    }

    @Override
    public void close() throws IOException {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }
}
