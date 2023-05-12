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

import org.apache.hadoop.hive.ql.io.RecordIdentifier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * Hive's RecordIdentifier implementation.
 */

public class PhoenixRowKey extends RecordIdentifier {

    private PrimaryKeyData rowKeyMap = PrimaryKeyData.EMPTY;

    public PhoenixRowKey() {

    }

    public void setRowKeyMap(Map<String, Object> rowKeyMap) {
        this.rowKeyMap = new PrimaryKeyData(rowKeyMap);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);

        rowKeyMap.serialize((OutputStream) dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);

        try {
            rowKeyMap = PrimaryKeyData.deserialize((InputStream) dataInput);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
