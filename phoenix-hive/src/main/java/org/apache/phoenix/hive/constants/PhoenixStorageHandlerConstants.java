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
package org.apache.phoenix.hive.constants;


/**
 * Constants using for Hive Storage Handler implementation
 */
public class PhoenixStorageHandlerConstants {

    public static final String HBASE_INPUT_FORMAT_CLASS = "phoenix.input.format.class";

    public static final String PHOENIX_TABLE_NAME = "phoenix.table.name";

    public static final String ZOOKEEPER_QUORUM = "phoenix.zookeeper.quorum";
    public static final String ZOOKEEPER_PORT = "phoenix.zookeeper.client.port";
    public static final String ZOOKEEPER_PARENT = "phoenix.zookeeper.znode.parent";
    public static final String DEFAULT_ZOOKEEPER_QUORUM = "localhost";
    public static final int DEFAULT_ZOOKEEPER_PORT = 2181;
    public static final String DEFAULT_ZOOKEEPER_PARENT = "/hbase";

    public static final String PHOENIX_ROWKEYS = "phoenix.rowkeys";
    public static final String PHOENIX_COLUMN_MAPPING = "phoenix.column.mapping";
    public static final String PHOENIX_TABLE_OPTIONS = "phoenix.table.options";

    public static final String PHOENIX_TABLE_QUERY_HINT = ".query.hint";
    public static final String PHOENIX_REDUCER_NUMBER = ".reducer.count";
    public static final String DISABLE_WAL = ".disable.wal";
    public static final String BATCH_MODE = "batch.mode";
    public static final String AUTO_FLUSH = ".auto.flush";

    public static final String COLON = ":";
    public static final String COMMA = ",";
    public static final String EMPTY_STRING = "";
    public static final String EQUAL = "=";
    public static final String IS = "is";
    public static final String QUESTION = "?";

    public static final String SPLIT_BY_STATS = "split.by.stats";
    public static final String HBASE_SCAN_CACHE = "hbase.scan.cache";
    public static final String HBASE_SCAN_CACHEBLOCKS = "hbase.scan.cacheblock";
    public static final String HBASE_DATE_FORMAT = "hbase.date.format";
    public static final String HBASE_TIMESTAMP_FORMAT = "hbase.timestamp.format";
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    public static final String DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    public static final String IN_OUT_WORK = "in.out.work";
    public static final String IN_WORK = "input";
    public static final String OUT_WORK = "output";
}
