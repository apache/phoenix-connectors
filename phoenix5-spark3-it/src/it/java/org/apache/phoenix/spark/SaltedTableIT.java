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
package org.apache.phoenix.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.end2end.salted.BaseSaltedTableIT;
import org.apache.phoenix.util.QueryBuilder;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class SaltedTableIT extends BaseSaltedTableIT {

    @Override
    protected ResultSet executeQueryThrowsException(Connection conn, QueryBuilder queryBuilder,
                                                    String expectedPhoenixExceptionMsg, String expectedSparkExceptionMsg) {
        ResultSet rs = null;
        try {
            rs = executeQuery(conn, queryBuilder);
            fail();
        }
        catch(Exception e) {
            assertTrue(e.getMessage().contains(expectedSparkExceptionMsg));
        }
        return rs;
    }

    @Override
    protected ResultSet executeQuery(Connection conn, QueryBuilder queryBuilder) throws SQLException {
        return SparkUtil.executeQuery(conn, queryBuilder, getUrl(), config);
    }

}