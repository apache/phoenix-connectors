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

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.RegionSizeCalculator;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.iterate.MapReduceParallelScanGrouper;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.spark.FilterExpressionCompiler;
import org.apache.phoenix.spark.SparkSchemaUtil;
import org.apache.phoenix.spark.datasource.v2.PhoenixDataSource;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class PhoenixDataSourceReader implements DataSourceReader, SupportsPushDownFilters,
        SupportsPushDownRequiredColumns {

    private final DataSourceOptions options;
    private final String tableName;
    private final String jdbcUrl;
    private final boolean dateAsTimestamp;
    private final Properties overriddenProps;
    private final boolean doNotMapColumnFamily;

    private StructType schema;
    private Filter[] pushedFilters = new Filter[]{};
    // derived from pushedFilters
    private String whereClause;

    public PhoenixDataSourceReader(DataSourceOptions options) {
        if (!options.tableName().isPresent()) {
            throw new RuntimeException("No Phoenix option " + DataSourceOptions.TABLE_KEY + " defined");
        }

        this.options = options;
        this.tableName = options.tableName().get();
        this.jdbcUrl = PhoenixDataSource.getJdbcUrlFromOptions(options);
        this.dateAsTimestamp = options.getBoolean(PhoenixDataSource.DATE_AS_TIME_STAMP, false);
        this.doNotMapColumnFamily = options.getBoolean(PhoenixDataSource.DO_NOT_MAP_COLUMN_FAMILY, false);
        this.overriddenProps = PhoenixDataSource.extractPhoenixHBaseConfFromOptions(options);
        setSchema();
    }

    /**
     * Sets the schema using all the table columns before any column pruning has been done
     */
    private void setSchema() {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, overriddenProps)) {
            List<ColumnInfo> columnInfos = generateColumnInfo(conn, tableName);
            Seq<ColumnInfo> columnInfoSeq = JavaConverters.asScalaIteratorConverter(columnInfos.iterator()).asScala().toSeq();
            schema = SparkSchemaUtil.phoenixSchemaToCatalystSchema(columnInfoSeq, dateAsTimestamp, doNotMapColumnFamily);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    PhoenixInputPartition getInputPartition(PhoenixDataSourceReadOptions readOptions,
            PhoenixInputSplit inputSplit) {
        return new PhoenixInputPartition(readOptions, schema, inputSplit);
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        Tuple3<String, Filter[], Filter[]> tuple3 = new FilterExpressionCompiler().pushFilters(filters);
        whereClause = tuple3._1();
        pushedFilters = tuple3._3();
        return tuple3._2();
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        Optional<String> currentScnValue = options.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE);
        Optional<String> tenantId = options.get(PhoenixDataSource.TENANT_ID);
        // Generate splits based off statistics, or just region splits?
        boolean splitByStats = options.getBoolean(
                PhoenixConfigurationUtil.MAPREDUCE_SPLIT_BY_STATS, PhoenixConfigurationUtil.DEFAULT_SPLIT_BY_STATS);
        if (currentScnValue.isPresent()) {
            overriddenProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, currentScnValue.get());
        }
        if (tenantId.isPresent()) {
            overriddenProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId.get());
        }
        try (Connection conn = DriverManager.getConnection(jdbcUrl, overriddenProps)) {
            List<ColumnInfo> columnInfos = PhoenixRuntime.generateColumnInfo(conn, tableName, new ArrayList<>(
                    Arrays.asList(schema.names())));
            final Statement statement = conn.createStatement();
            final String selectStatement = QueryUtil.constructSelectStatement(tableName, columnInfos, whereClause);
            if (selectStatement == null) {
                throw new NullPointerException();
            }

            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            // Optimize the query plan so that we potentially use secondary indexes
            final QueryPlan queryPlan = pstmt.optimizeQuery(selectStatement);
            final Scan scan = queryPlan.getContext().getScan();

            // setting the snapshot configuration
            Optional<String> snapshotName = options.get(PhoenixConfigurationUtil.SNAPSHOT_NAME_KEY);
            if (snapshotName.isPresent())
                PhoenixConfigurationUtil.setSnapshotNameKey(queryPlan.getContext().getConnection().
                        getQueryServices().getConfiguration(), snapshotName.get());

            // Initialize the query plan so it sets up the parallel scans
            queryPlan.iterator(MapReduceParallelScanGrouper.getInstance());

            List<KeyRange> allSplits = queryPlan.getSplits();
            // Get the RegionSizeCalculator
            PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
            org.apache.hadoop.hbase.client.Connection connection =
                    phxConn.getQueryServices().getAdmin().getConnection();
            RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(queryPlan
                    .getTableRef().getTable().getPhysicalName().toString()));

            final List<InputPartition<InternalRow>> partitions = new ArrayList<>(allSplits.size());
            for (List<Scan> scans : queryPlan.getScans()) {
                // Get the region location
                HRegionLocation location = regionLocator.getRegionLocation(
                        scans.get(0).getStartRow(),
                        false
                );

                String regionLocation = location.getHostname();

                // Get the region size
                RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(regionLocator, connection.getAdmin());
                long regionSize = sizeCalculator.getRegionSize(location.getRegionInfo().getRegionName());
                byte[] pTableCacheBytes = PTableImpl.toProto(queryPlan.getTableRef().getTable()).
                        toByteArray();
                PhoenixDataSourceReadOptions phoenixDataSourceOptions =
                        new PhoenixDataSourceReadOptions(jdbcUrl, currentScnValue.orElse(null),
                                tenantId.orElse(null), selectStatement, overriddenProps,
                                pTableCacheBytes);
                if (splitByStats) {
                    for (Scan aScan : scans) {
                        partitions.add(getInputPartition(phoenixDataSourceOptions,
                                new PhoenixInputSplit(Collections.singletonList(aScan), regionSize, regionLocation)));
                    }
                } else {
                    partitions.add(getInputPartition(phoenixDataSourceOptions,
                            new PhoenixInputSplit(scans, regionSize, regionLocation)));
                }
            }
            return partitions;
        } catch (Exception e) {
            throw new RuntimeException("Unable to plan query", e);
        }
    }

    @Override
    public Filter[] pushedFilters() {
        return pushedFilters;
    }

    @Override
    public void pruneColumns(StructType schema) {
        if (schema.fields() != null && schema.fields().length != 0)
            this.schema = schema;
    }

    //TODO Method PhoenixRuntime.generateColumnInfo skip only salt column, add skip tenant_id column.
    private List<ColumnInfo> generateColumnInfo(Connection conn, String tableName) throws SQLException {
        List<ColumnInfo> columnInfos = new ArrayList<>();
        PTable table = PhoenixRuntime.getTable(conn, SchemaUtil.normalizeFullTableName(tableName));
        int startOffset = 0;

        if (table.getTenantId() != null) {
            startOffset++;
        }
        if (table.getBucketNum() != null) {
            startOffset++;
        }

        for (int offset = startOffset; offset < table.getColumns().size(); offset++) {
            PColumn column = table.getColumns().get(offset);
            columnInfos.add(PhoenixRuntime.getColumnInfo(column));
        }
        return columnInfos;
    }
}
