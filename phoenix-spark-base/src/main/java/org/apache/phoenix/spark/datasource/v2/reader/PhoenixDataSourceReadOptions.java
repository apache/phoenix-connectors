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

import java.io.Serializable;
import java.util.Properties;

class PhoenixDataSourceReadOptions implements Serializable {

    private final String tenantId;
    private final String zkUrl;
    private final String scn;
    private final String selectStatement;
    private final Properties overriddenProps;
    private final byte[] pTableCacheBytes;

    PhoenixDataSourceReadOptions(String zkUrl, String scn, String tenantId,
            String selectStatement, Properties overriddenProps, byte[] pTableCacheBytes) {
        if(overriddenProps == null){
            throw new NullPointerException();
        }
        this.zkUrl = zkUrl;
        this.scn = scn;
        this.tenantId = tenantId;
        this.selectStatement = selectStatement;
        this.overriddenProps = overriddenProps;
        this.pTableCacheBytes = pTableCacheBytes;
    }

    String getSelectStatement() {
        return selectStatement;
    }

    String getScn() {
        return scn;
    }

    String getZkUrl() {
        return zkUrl;
    }

    String getTenantId() {
        return tenantId;
    }

    Properties getOverriddenProps() {
        return overriddenProps;
    }

    byte[] getPTableCacheBytes() {
        return pTableCacheBytes;
    }
}
