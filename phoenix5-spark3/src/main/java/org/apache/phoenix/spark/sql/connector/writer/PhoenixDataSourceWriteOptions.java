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

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Properties;


//TODO Factor out common code with PhoenixDataSourceReadOptions to common abstract superclass
class PhoenixDataSourceWriteOptions implements Serializable {

    private final String tableName;
    private final String jdbcUrl;
    private final String tenantId;
    private final String scn;
    private final StructType schema;
    private final boolean skipNormalizingIdentifier;
    private final Properties overriddenProps;

    private PhoenixDataSourceWriteOptions(String tableName, String jdbcUrl, String scn,
                                          String tenantId, StructType schema, boolean skipNormalizingIdentifier,
                                          Properties overriddenProps) {
        if (tableName == null) {
            throw new IllegalArgumentException("tableName must not be null");
        }
        if (jdbcUrl == null) {
            throw new IllegalArgumentException("jdbcUrl must not be null");
        }
        if (schema == null) {
            throw new IllegalArgumentException("schema must not be null");
        }
        if (overriddenProps == null) {
            throw new IllegalArgumentException("overriddenProps must not be null");
        }
        this.tableName = tableName;
        this.jdbcUrl = jdbcUrl;
        this.scn = scn;
        this.tenantId = tenantId;
        this.schema = schema;
        this.skipNormalizingIdentifier = skipNormalizingIdentifier;
        this.overriddenProps = overriddenProps;
    }

    String getScn() {
        return scn;
    }

    String getJdbcUrl() {
        return jdbcUrl;
    }

    String getTenantId() {
        return tenantId;
    }

    StructType getSchema() {
        return schema;
    }

    String getTableName() {
        return tableName;
    }

    boolean skipNormalizingIdentifier() {
        return skipNormalizingIdentifier;
    }

    Properties getEffectiveProps() {
        String scn = getScn();
        String tenantId = getTenantId();
        if (scn != null) {
            overriddenProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, scn);
        }
        if (tenantId != null) {
            overriddenProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        return overriddenProps;
    }

    static class Builder {
        private String tableName;
        private String jdbcUrl;
        private String scn;
        private String tenantId;
        private StructType schema;
        private boolean skipNormalizingIdentifier;
        private Properties overriddenProps = new Properties();

        Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        Builder setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        Builder setScn(String scn) {
            this.scn = scn;
            return this;
        }

        Builder setTenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        Builder setSchema(StructType schema) {
            this.schema = schema;
            return this;
        }

        Builder setSkipNormalizingIdentifier(boolean skipNormalizingIdentifier) {
            this.skipNormalizingIdentifier = skipNormalizingIdentifier;
            return this;
        }

        Builder setOverriddenProps(Properties overriddenProps) {
            this.overriddenProps = overriddenProps;
            return this;
        }

        PhoenixDataSourceWriteOptions build() {
            return new PhoenixDataSourceWriteOptions(tableName, jdbcUrl, scn, tenantId, schema,
                    skipNormalizingIdentifier, overriddenProps);
        }
    }
}