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
package org.apache.phoenix.spark.datasource.v2;

import java.util.Optional;
import java.util.Properties;

import org.apache.phoenix.spark.datasource.v2.reader.PhoenixDataSourceReader;
import org.apache.phoenix.spark.datasource.v2.writer.PhoenixDataSourceWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;

/**
 * Implements the DataSourceV2 api to read and write from Phoenix tables
 */
public class PhoenixDataSource  implements DataSourceV2,  ReadSupport, WriteSupport, DataSourceRegister {

    private static final Logger logger = LoggerFactory.getLogger(PhoenixDataSource.class);
    public static final String SKIP_NORMALIZING_IDENTIFIER = "skipNormalizingIdentifier";
    public static final String ZOOKEEPER_URL = "zkUrl";
    public static final String PHOENIX_CONFIGS = "phoenixconfigs";

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        return new PhoenixDataSourceReader(options);
    }

    @Override
    public Optional<DataSourceWriter> createWriter(String writeUUID, StructType schema,
            SaveMode mode, DataSourceOptions options) {
        return Optional.of(new PhoenixDataSourceWriter(mode, schema, options));
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
    public static Properties extractPhoenixHBaseConfFromOptions(final DataSourceOptions options) {
        Properties confToSet = new Properties();
        if (options != null) {
            Optional phoenixConfigs = options.get(PHOENIX_CONFIGS);
            if (phoenixConfigs.isPresent()) {
                String phoenixConf = String.valueOf(phoenixConfigs.get());
                String[] confs = phoenixConf.split(",");
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
