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

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.spark.datasource.v2.PhoenixDataSource.PHOENIX_CONFIGS;
import static org.apache.phoenix.spark.datasource.v2.PhoenixDataSource.extractPhoenixHBaseConfFromOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PhoenixDataSourceTest {

    private static final String P1 = "p1";
    private static final String P2 = "p2";
    private static final String P3 = "p3";
    private static final String V1 = "v1";
    private static final String V2 = "v2";
    private static final String V3 = "v3";
    private static final String EQ = "=";
    private static final String COMMA = ",";
    private static final String SINGLE_PHOENIX_PROP = P1 + EQ + V1;
    private static final String VALID_PHOENIX_PROPS_LIST =
            SINGLE_PHOENIX_PROP + COMMA + P2 + EQ + V2 + COMMA + P3 + EQ + V3;
    private static final String INVALID_PHOENIX_PROPS_LIST =
            SINGLE_PHOENIX_PROP + COMMA + P2 + V2 + COMMA + P3 + EQ + V3;

    @Test
    public void testExtractSinglePhoenixProp() {
        Map<String, String> props = new HashMap<>();
        props.put(PHOENIX_CONFIGS, SINGLE_PHOENIX_PROP);
        Properties p = extractPhoenixHBaseConfFromOptions(new DataSourceOptions(props));
        assertEquals(V1, p.getProperty(P1));
    }

    @Test
    public void testPhoenixConfigsExtractedProperly() {
        Map<String, String> props = new HashMap<>();
        // Add another random option
        props.put("k", "v");
        props.put(PHOENIX_CONFIGS, VALID_PHOENIX_PROPS_LIST);
        Properties p = extractPhoenixHBaseConfFromOptions(new DataSourceOptions(props));
        assertEquals(V1, p.getProperty(P1));
        assertEquals(V2, p.getProperty(P2));
        assertEquals(V3, p.getProperty(P3));
    }

    @Test
    public void testInvalidConfThrowsException() {
        Map<String, String> props = new HashMap<>();
        props.put(PHOENIX_CONFIGS, INVALID_PHOENIX_PROPS_LIST);
        try {
            extractPhoenixHBaseConfFromOptions(new DataSourceOptions(props));
            fail("Should have thrown an exception!");
        } catch (RuntimeException rte) {
            assertTrue(rte.getCause() instanceof ArrayIndexOutOfBoundsException);
        }
    }

    @Test
    public void testNullOptionsReturnsEmptyMap() {
        assertTrue(extractPhoenixHBaseConfFromOptions(null).isEmpty());
    }

}
