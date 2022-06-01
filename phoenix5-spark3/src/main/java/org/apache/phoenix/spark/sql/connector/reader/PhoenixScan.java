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
package org.apache.phoenix.spark.sql.connector.reader;

import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.phoenix.compat.CompatUtil;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.iterate.MapReduceParallelScanGrouper;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.spark.sql.connector.PhoenixDataSource;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.spark.sql.connector.PhoenixDataSource.extractPhoenixHBaseConfFromOptions;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

public class PhoenixScan implements Scan, Batch {

    private final StructType schema;
    private final CaseInsensitiveStringMap options;
    private final String zkUrl;
    private final Properties overriddenProps;
    private PhoenixDataSourceReadOptions phoenixDataSourceOptions;
    private final String tableName;
    private String currentScnValue;
    private String tenantId;
    private boolean splitByStats;
    private final String whereClause;

    PhoenixScan(StructType schema, CaseInsensitiveStringMap options, String whereClause) {
        this.schema = schema;
        this.options = options;
        this.whereClause = whereClause;
        this.overriddenProps = extractPhoenixHBaseConfFromOptions(options);
        this.zkUrl = options.get(PhoenixDataSource.ZOOKEEPER_URL);
        tableName = options.get("table");
    }

    private void populateOverriddenProperties(){
        currentScnValue = options.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE);
        tenantId = options.get(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID);
        // Generate splits based off statistics, or just region splits?
        splitByStats = options.getBoolean(
                PhoenixConfigurationUtil.MAPREDUCE_SPLIT_BY_STATS, PhoenixConfigurationUtil.DEFAULT_SPLIT_BY_STATS);
        if(currentScnValue != null) {
            overriddenProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, currentScnValue);
        }
        if (tenantId != null){
            overriddenProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public String description() {
        return this.getClass().toString();
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        populateOverriddenProperties();
        try (Connection conn = DriverManager.getConnection(
                JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + zkUrl, overriddenProps)) {
            List<ColumnInfo> columnInfos = PhoenixRuntime.generateColumnInfo(conn, tableName, new ArrayList<>(
                    Arrays.asList(schema.names())));
            final Statement statement = conn.createStatement();
            final String selectStatement = QueryUtil.constructSelectStatement(tableName, columnInfos, whereClause);
            if (selectStatement == null){
                throw new NullPointerException();
            }
            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            // Optimize the query plan so that we potentially use secondary indexes
            final QueryPlan queryPlan = pstmt.optimizeQuery(selectStatement);
            final org.apache.hadoop.hbase.client.Scan scan = queryPlan.getContext().getScan();

            // Initialize the query plan so it sets up the parallel scans
            queryPlan.iterator(MapReduceParallelScanGrouper.getInstance());

            List<KeyRange> allSplits = queryPlan.getSplits();
            // Get the RegionSizeCalculator
            PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
            org.apache.hadoop.hbase.client.Connection connection =
                    phxConn.getQueryServices().getAdmin().getConnection();
            RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(queryPlan
                    .getTableRef().getTable().getPhysicalName().toString()));

            final InputPartition[] partitions = new PhoenixInputPartition[allSplits.size()];
            int partitionCount = 0;

            for (List<org.apache.hadoop.hbase.client.Scan> scans : queryPlan.getScans()) {
                // Get the region location
                HRegionLocation location = regionLocator.getRegionLocation(
                        scans.get(0).getStartRow(),
                        false
                );

                String regionLocation = location.getHostname();

                // Get the region size
                long regionSize = CompatUtil.getSize(regionLocator, connection.getAdmin(), location);
                byte[] pTableCacheBytes = PTableImpl.toProto(queryPlan.getTableRef().getTable()).
                    toByteArray();
                phoenixDataSourceOptions =
                        new PhoenixDataSourceReadOptions(zkUrl, currentScnValue,
                                tenantId, selectStatement, overriddenProps, pTableCacheBytes);

                if (splitByStats) {
                    for (org.apache.hadoop.hbase.client.Scan aScan : scans) {
                        partitions[partitionCount++] = new PhoenixInputPartition(
                                new PhoenixInputSplit(Collections.singletonList(aScan), regionSize, regionLocation));
                    }
                } else {
                    partitions[partitionCount++] = new PhoenixInputPartition(
                            new PhoenixInputSplit(scans, regionSize, regionLocation));
                }
            }
            return partitions;
        } catch (Exception e) {
            throw new RuntimeException("Unable to plan query", e);
        }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new PhoenixPartitionReadFactory(phoenixDataSourceOptions, schema);
    }

    @VisibleForTesting
    PhoenixDataSourceReadOptions getOptions() {
        return phoenixDataSourceOptions;
    }
}
