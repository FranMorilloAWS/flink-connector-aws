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

package org.apache.flink.formats.protobuf.glue.schema.registry;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.util.List;

/**
 * Converts Flink {@link RowData} to Protobuf {@link DynamicMessage} based on a Protobuf {@link
 * Descriptors.Descriptor}.
 */
@Internal
public class RowDataToProtobufConverter {

    /**
     * Converts a Flink RowData to a Protobuf DynamicMessage.
     *
     * @param rowData the Flink RowData
     * @param rowType the Flink RowType describing the schema
     * @param descriptor the Protobuf message descriptor
     * @return a DynamicMessage
     */
    public static DynamicMessage convertRowData(
            RowData rowData, RowType rowType, Descriptors.Descriptor descriptor) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        List<Descriptors.FieldDescriptor> fields = descriptor.getFields();

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (rowData.isNullAt(i)) {
                // proto3 default: skip null fields (they use default values)
                continue;
            }
            LogicalType fieldType = rowType.getTypeAt(i);
            Descriptors.FieldDescriptor fd = fields.get(i);
            Object value = extractFieldValue(rowData, i, fieldType);
            if (value != null) {
                builder.setField(fd, value);
            }
        }
        return builder.build();
    }

    private static Object extractFieldValue(RowData rowData, int index, LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return rowData.getBoolean(index);
            case TINYINT:
                return (int) rowData.getByte(index);
            case SMALLINT:
                return (int) rowData.getShort(index);
            case INTEGER:
                return rowData.getInt(index);
            case BIGINT:
                return rowData.getLong(index);
            case FLOAT:
                return rowData.getFloat(index);
            case DOUBLE:
                return rowData.getDouble(index);
            case CHAR:
            case VARCHAR:
                return rowData.getString(index).toString();
            case BINARY:
            case VARBINARY:
                return ByteString.copyFrom(rowData.getBinary(index));
            default:
                // For unsupported types, convert to string representation
                return rowData.getString(index).toString();
        }
    }

    private RowDataToProtobufConverter() {}
}
