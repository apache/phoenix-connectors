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
package org.apache.phoenix.spark.sql.connector.writer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;


import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.execution.datasources.SparkJdbcUtil;
import org.apache.spark.sql.execution.datasources.jdbc.PhoenixJdbcDialect$;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Attribute;

import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.DEFAULT_UPSERT_BATCH_SIZE;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.UPSERT_BATCH_SIZE;

public class PhoenixDataWriter implements DataWriter<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(PhoenixDataWriter.class);
    private final StructType schema;
    private final Connection conn;
    private final PreparedStatement statement;
    private final long batchSize;
    private long numRecords = 0;
    private ExpressionEncoder<Row> encoder;

    PhoenixDataWriter(StructType schema, PhoenixDataSourceWriteOptions options) {
        String jdbcUrl = options.getJdbcUrl();
        Properties connectionProps = options.getEffectiveProps();
        this.schema = options.getSchema();

        List<Attribute> attrs = new ArrayList<>();
        for (AttributeReference ref : scala.collection.JavaConverters.seqAsJavaListConverter(
            DataTypeUtils.toAttributes(schema)).asJava()) {
            attrs.add(ref.toAttribute());
        }
        encoder = ExpressionEncoder.apply(schema).resolveAndBind( scala.collection.JavaConverters.asScalaIteratorConverter(attrs.iterator()).asScala().toSeq(), SimpleAnalyzer$.MODULE$);
        try {
            this.conn = DriverManager.getConnection(jdbcUrl, connectionProps);
            List<String> colNames =  new ArrayList<>(Arrays.asList(options.getSchema().names()));
            if (!options.skipNormalizingIdentifier()){
                colNames = colNames.stream().map(SchemaUtil::normalizeIdentifier).collect(Collectors.toList());
            }
            String upsertSql = QueryUtil.constructUpsertStatement(options.getTableName(), colNames, null);
            this.statement = this.conn.prepareStatement(upsertSql);
            // Configure batch size to 0 or negative value to disable intermediate or batch commits in task.
            // So that commit can be called only once at the end to task execution.
            // This helps ensure consistent state of database when failures occurred and retried
            // mainly when transactions enabled.
            this.batchSize = Long.parseLong(connectionProps.getProperty(UPSERT_BATCH_SIZE,
                    String.valueOf(DEFAULT_UPSERT_BATCH_SIZE)));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void commitBatchUpdates() throws SQLException {
        conn.commit();
    }

    @Override
    public void write(InternalRow internalRow) throws IOException {
        try {
            int i=0;
            Row row = SparkJdbcUtil.toRow(encoder, internalRow);
            for (StructField field : schema.fields()) {
                DataType dataType = field.dataType();
                if (internalRow.isNullAt(i)) {
                    statement.setNull(i + 1, SparkJdbcUtil.getJdbcType(dataType,
                            PhoenixJdbcDialect$.MODULE$).jdbcNullType());
                } else {
                	SparkJdbcUtil.makeSetter(conn, PhoenixJdbcDialect$.MODULE$, dataType).apply(statement, row, i);
                }
                ++i;
            }
            numRecords++;
            statement.execute();
            // Run batch wise commits only when the batch size is positive value.
            // Otherwise commit gets called at the end of task
            if (batchSize > 0 && numRecords % batchSize == 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("commit called on a batch of size : " + batchSize);
                }
                commitBatchUpdates();
            }
        } catch (SQLException e) {
            throw new IOException("Exception while executing Phoenix prepared statement", e);
        }
    }

    @Override
    public WriterCommitMessage commit() {
        try {
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                statement.close();
                conn.close();
            }
            catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        return null;
    }

    @Override
    public void abort() {
        try {
            // To rollback any ongoing transactions
            conn.rollback();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        try {
            conn.close();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
}
