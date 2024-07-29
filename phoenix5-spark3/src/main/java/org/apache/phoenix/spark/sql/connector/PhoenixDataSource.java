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
package org.apache.phoenix.spark.sql.connector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.phoenix.spark.SparkSchemaUtil;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

/**
 * Implements the DataSourceV2 api to read and write from Phoenix tables
 */
public class PhoenixDataSource implements TableProvider, DataSourceRegister {

    private static final Logger logger = LoggerFactory.getLogger(PhoenixDataSource.class);
    public static final String SKIP_NORMALIZING_IDENTIFIER = "skipNormalizingIdentifier";
    @Deprecated
    public static final String ZOOKEEPER_URL = "zkUrl";
    public static final String JDBC_URL = "jdbcUrl";
    public static final String PHOENIX_CONFIGS = "phoenixconfigs";
    protected StructType schema;

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options){
        if (options.get("table") == null) {
            throw new RuntimeException("No Phoenix option " + "Table" + " defined");
        }

        String jdbcUrl = getJdbcUrlFromOptions(options);
        String tableName = options.get("table");
        String tenant = options.get(PhoenixRuntime.TENANT_ID_ATTRIB);
        boolean dateAsTimestamp = Boolean.parseBoolean(options.getOrDefault("dateAsTimestamp", Boolean.toString(false)));
        boolean doNotMapColumnFamily = Boolean.parseBoolean(options.getOrDefault("doNotMapColumnFamily", Boolean.toString(false)));
        Properties overriddenProps = extractPhoenixHBaseConfFromOptions(options);
        if (tenant != null) {
            overriddenProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenant);
        }

        /**
         * Sets the schema using all the table columns before any column pruning has been done
         */
        try (Connection conn = DriverManager.getConnection(jdbcUrl, overriddenProps)) {
            List<ColumnInfo> columnInfos = PhoenixRuntime.generateColumnInfo(conn, tableName, null);
            Seq<ColumnInfo> columnInfoSeq = JavaConverters.asScalaIteratorConverter(columnInfos.iterator()).asScala().toSeq();
            schema = SparkSchemaUtil.phoenixSchemaToCatalystSchema(columnInfoSeq, dateAsTimestamp, doNotMapColumnFamily);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return schema;
    }

    public static String getJdbcUrlFromOptions(Map<String, String> options) {
        if (options.get(JDBC_URL) != null && options.get(ZOOKEEPER_URL) != null) {
            throw new RuntimeException("If " + JDBC_URL + " is specified, then  " + ZOOKEEPER_URL
                    + " must not be specified");
        }

        String jdbcUrl = options.get(JDBC_URL);
        String zkUrl = options.get(ZOOKEEPER_URL);
        // Backward compatibility logic
        if (jdbcUrl == null) {
            if (zkUrl != null) {
                if (zkUrl.startsWith(JDBC_PROTOCOL)) {
                    // full URL specified, use it
                    jdbcUrl = zkUrl;
                } else {
                    // backwards compatibility, assume ZK, and missing protocol
                    // Don't use the specific protocol, as we need to work with older releases.
                    jdbcUrl = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + zkUrl;
                }
            } else {
                jdbcUrl = JDBC_PROTOCOL;
            }
        }
        return jdbcUrl;
    }

    @Override
    public Table getTable( StructType schema, Transform[] transforms, Map<String, String> properties)
    {
        return new PhoenixTable(schema, properties);
    }

    /**
     * Extract HBase and Phoenix properties that need to be set in both the driver and workers.
     * We expect these properties to be passed against the key
     * {@link PhoenixDataSource#PHOENIX_CONFIGS}. The corresponding value should be a
     * comma-separated string containing property names and property values. For example:
     * prop1=val1,prop2=val2,prop3=val3
     * @param options DataSource options passed in
     * @return Properties map
     */
    public static Properties extractPhoenixHBaseConfFromOptions(final Map<String,String> options) {
        Properties confToSet = new Properties();
        if (options != null) {
            String phoenixConfigs = options.get(PHOENIX_CONFIGS);
            if (phoenixConfigs != null) {
                String[] confs = phoenixConfigs.split(",");
                for (String conf : confs) {
                    String[] confKeyVal = conf.split("=");
                    try {
                        confToSet.setProperty(confKeyVal[0], confKeyVal[1]);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        throw new RuntimeException("Incorrect format for phoenix/HBase configs. "
                                + "Expected format: <prop1>=<val1>,<prop2>=<val2>,<prop3>=<val3>..",
                                e);
                    }
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Got the following Phoenix/HBase config:\n" + confToSet);
            }
        }
        return confToSet;
    }

    @Override
    public String shortName() {
        return "phoenix";
    }
}
