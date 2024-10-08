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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.mapreduce.PhoenixResultWritable;
import org.apache.phoenix.hive.util.PhoenixConnectionUtil;
import org.apache.phoenix.hive.util.PhoenixStorageHandlerUtil;
import org.apache.phoenix.hive.util.PhoenixUtil;
import org.apache.phoenix.util.ColumnInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Serializer used in PhoenixSerDe and PhoenixRecordUpdater to produce Writable.
 */
public class PhoenixSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixSerializer.class);

    public static enum DmlType {
        NONE,
        SELECT,
        INSERT,
        UPDATE,
        DELETE
    }

    private int columnCount = 0;
    private PhoenixResultWritable pResultWritable;

    public PhoenixSerializer(Configuration config, Properties tbl) throws SerDeException {
        String mapping = tbl.getProperty(PhoenixStorageHandlerConstants.PHOENIX_COLUMN_MAPPING,
          null);
        if(mapping != null) {
            config.set(PhoenixStorageHandlerConstants.PHOENIX_COLUMN_MAPPING, mapping);
        }

        // Populate the table properties into config, because these values are used in
        // the initialization of PhoenixResultWritable if the table is transactional
        String tableName = tbl.getProperty(PhoenixStorageHandlerConstants.PHOENIX_TABLE_NAME);
        config.set(PhoenixStorageHandlerConstants.PHOENIX_TABLE_NAME, tableName);

        config.set(PhoenixStorageHandlerConstants.PHOENIX_ROWKEYS,
            tbl.getProperty(PhoenixStorageHandlerConstants.PHOENIX_ROWKEYS));

        String quorum = config.get(PhoenixStorageHandlerConstants.ZOOKEEPER_QUORUM);
        if (quorum == null) {
            config.set(PhoenixStorageHandlerConstants.ZOOKEEPER_QUORUM,
              tbl.getProperty(PhoenixStorageHandlerConstants.ZOOKEEPER_QUORUM,
                PhoenixStorageHandlerConstants.DEFAULT_ZOOKEEPER_QUORUM));
        }

        int zooKeeperClientPort =
          config.getInt(PhoenixStorageHandlerConstants.ZOOKEEPER_PORT, 0);
        if (zooKeeperClientPort == 0) {
            config.setInt(PhoenixStorageHandlerConstants.ZOOKEEPER_PORT,
              Integer.parseInt(tbl.getProperty(PhoenixStorageHandlerConstants.ZOOKEEPER_PORT,
                String.valueOf(PhoenixStorageHandlerConstants.DEFAULT_ZOOKEEPER_PORT))));
        }

        String zNodeParent = config.get(PhoenixStorageHandlerConstants.ZOOKEEPER_PARENT);
        if (zNodeParent == null) {
            config.set(PhoenixStorageHandlerConstants.ZOOKEEPER_PARENT,
              tbl.getProperty(PhoenixStorageHandlerConstants.ZOOKEEPER_PARENT,
                PhoenixStorageHandlerConstants.DEFAULT_ZOOKEEPER_PARENT));
        }

        try (Connection conn = PhoenixConnectionUtil.getInputConnection(config, tbl)) {
            List<ColumnInfo> columnMetadata = PhoenixUtil.getColumnInfoList(conn, tableName);

            columnCount = columnMetadata.size();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Column-meta : " + columnMetadata);
            }

            pResultWritable = new PhoenixResultWritable(config, columnMetadata);
        } catch (SQLException | IOException e) {
            throw new SerDeException(e);
        }
    }

    public Writable serialize(Object values, ObjectInspector objInspector, DmlType dmlType) {
        pResultWritable.clear();

        final StructObjectInspector structInspector = (StructObjectInspector) objInspector;
        final List<? extends StructField> fieldList = structInspector.getAllStructFieldRefs();

        if (LOG.isTraceEnabled()) {
            LOG.trace("FieldList : " + fieldList + " values(" + values.getClass() + ") : " +
                    values);
        }

        int fieldCount = columnCount;
        if (dmlType == DmlType.UPDATE || dmlType == DmlType.DELETE) {
            fieldCount++;
        }

        for (int i = 0; i < fieldCount; i++) {
            if (fieldList.size() <= i) {
                break;
            }

            StructField structField = fieldList.get(i);

            if (LOG.isTraceEnabled()) {
                LOG.trace("structField[" + i + "] : " + structField);
            }

            if (structField != null) {
                Object fieldValue = structInspector.getStructFieldData(values, structField);
                ObjectInspector fieldOI = structField.getFieldObjectInspector();

                String fieldName = structField.getFieldName();

                if (LOG.isTraceEnabled()) {
                    LOG.trace("Field " + fieldName + "[" + i + "] : " + fieldValue + ", " +
                            fieldOI);
                }

                Object value = null;
                switch (fieldOI.getCategory()) {
                    case PRIMITIVE:
                        value = ((PrimitiveObjectInspector) fieldOI).getPrimitiveJavaObject
                                (fieldValue);

                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Field " + fieldName + "[" + i + "] : " + value + "(" + value
                                    .getClass() + ")");
                        }

                        if (value instanceof HiveDecimal) {
                            value = ((HiveDecimal) value).bigDecimalValue();
                        } else if (value instanceof HiveChar) {
                            value = ((HiveChar) value).getValue().trim();
                        } else {
                            value = getDateOrTimestampValue(value);
                        }

                        pResultWritable.add(value);
                        break;
                    case LIST:
                    // Not support for arrays in insert statement yet
                        break;
                    case STRUCT:
                        if (dmlType == DmlType.DELETE) {
                            // When update/delete, First value is struct<transactionid:bigint,
                            // bucketid:int,rowid:bigint,primaryKey:binary>>
                            List<Object> fieldValueList = ((StandardStructObjectInspector)
                                    fieldOI).getStructFieldsDataAsList(fieldValue);

                            // convert to map from binary of primary key.
                            @SuppressWarnings("unchecked")
                            Map<String, Object> primaryKeyMap = (Map<String, Object>)
                                    PhoenixStorageHandlerUtil.toMap(((BytesWritable)
                                            fieldValueList.get(3)).getBytes());
                            for (Object pkValue : primaryKeyMap.values()) {
                                pResultWritable.add(pkValue);
                            }
                        }

                        break;
                    default:
                        new SerDeException("Phoenix Unsupported column type: " + fieldOI
                                .getCategory());
                }
            }
        }

        return pResultWritable;
    }
    
    private Object getDateOrTimestampValue(Object value){
        if (value instanceof Date) {
            value = java.sql.Date.valueOf(value.toString());
        } else if (value instanceof Timestamp) {
            value = java.sql.Timestamp.valueOf(value.toString());
        }
        return value;
    }
}
