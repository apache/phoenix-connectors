/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.hive;

import org.apache.hadoop.hive.ql.QTestMiniClusters;
import org.junit.BeforeClass;
import org.junit.Ignore;

@Ignore
// This time, we cannot run tests because of unshaded protobuf conflict between 
// HBase (2.5.0) and Tez (3.x)
public class HiveTezIT extends HivePhoenixStoreIT {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setup(QTestMiniClusters.MiniClusterType.TEZ);
    }
}
