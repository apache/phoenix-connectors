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
package org.apache.phoenix.compat;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.RegionSizeCalculator;

import java.io.IOException;

public class CompatUtil {

    private CompatUtil() {
        // Not to be instantiated
    }

    public static boolean tableExists(HBaseAdmin admin, String fullTableName)
            throws IOException {
        try {
            return admin.tableExists(fullTableName);
        } finally {
            admin.close();
        }
    }

    public static long getSize(RegionLocator regionLocator, Admin admin,
                               HRegionLocation location) throws IOException {
        RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(regionLocator, admin);
        return sizeCalculator.getRegionSize(location.getRegionInfo().getRegionName());
    }

    public static byte[] getTableName(byte[] tableNameBytes) {
        return tableNameBytes;
    }

    public static boolean isPhoenix4() {
        return true;
    }

    public static boolean isPhoenix5() {
        return false;
    }
}
