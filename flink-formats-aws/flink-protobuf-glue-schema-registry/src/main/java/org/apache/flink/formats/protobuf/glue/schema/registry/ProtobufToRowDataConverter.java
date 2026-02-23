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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.util.List;

/**
 * Converts Protobuf {@link DynamicMessage} to Flink {@link RowData} based on a Flink {@link
 * RowType}.
 */
@Internal
public class ProtobufToRowDataConverter {

    /**
     * Converts a Protobuf DynamicMessage to a Flink RowData.
     *
     * @param message the Protobuf DynamicMessage
     * @param rowType the Flink RowType describing the expected schema
     * @return a GenericRowData
     */
    public static RowData convertToRowData(DynamicMessage message, RowType rowType) {
        GenericRowData row = new GenericRowData(rowType.getFieldCount());
        List<Descriptors.FieldDescriptor> fields = message.getDescriptorForType().getFields();

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            LogicalType fieldType = rowType.getTypeAt(i);
            Descriptors.FieldDescriptor fd = fields.get(i);
            Object protoValue = message.getField(fd);
            row.setField(i, convertProtoValue(protoValue, fieldType, fd));
        }
        return row;
    }

    private static Object convertProtoValue(
            Object protoValue, LogicalType type, Descriptors.FieldDescriptor fd) {
        // In proto3, unset fields return default values (not null).
        // We treat default values as actual values (proto3 semantics).
        if (protoValue == null) {
            return null;
        }

        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return protoValue;
            case TINYINT:
                return ((Integer) protoValue).byteValue();
            case SMALLINT:
                return ((Integer) protoValue).shortValue();
            case INTEGER:
                return protoValue;
            case BIGINT:
                return protoValue;
            case FLOAT:
                return protoValue;
            case DOUBLE:
                return protoValue;
            case CHAR:
            case VARCHAR:
                return StringData.fromString(protoValue.toString());
            case BINARY:
            case VARBINARY:
                if (protoValue instanceof ByteString) {
                    return ((ByteString) protoValue).toByteArray();
                }
                return protoValue;
            default:
                return StringData.fromString(protoValue.toString());
        }
    }

    private ProtobufToRowDataConverter() {}
}
