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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.DescriptorProtos;

/**
 * Converts a Flink {@link RowType} to a Protobuf schema definition string and to a {@link
 * DescriptorProtos.FileDescriptorProto} for runtime use.
 */
@Internal
public class ProtobufSchemaConverter {

    private static final String PROTO_SYNTAX = "proto3";

    /**
     * Converts a Flink RowType to a Protobuf schema definition string (proto3 syntax).
     *
     * @param rowType the Flink RowType
     * @param messageName the name for the Protobuf message
     * @return a Protobuf schema definition string
     */
    public static String convertToProtobufSchema(RowType rowType, String messageName) {
        String sanitized = sanitizeMessageName(messageName);
        StringBuilder sb = new StringBuilder();
        sb.append("syntax = \"proto3\";\n\n");
        sb.append("message ").append(sanitized).append(" {\n");
        int fieldNumber = 1;
        for (RowType.RowField field : rowType.getFields()) {
            String protoType = toProtoType(field.getType());
            sb.append("  ").append(protoType).append(" ").append(field.getName());
            sb.append(" = ").append(fieldNumber++).append(";\n");
        }
        sb.append("}\n");
        return sb.toString();
    }

    /**
     * Builds a {@link DescriptorProtos.FileDescriptorProto} from a Flink RowType for runtime
     * Protobuf serialization/deserialization.
     *
     * @param rowType the Flink RowType
     * @param messageName the name for the Protobuf message
     * @return a FileDescriptorProto
     */
    public static DescriptorProtos.FileDescriptorProto buildFileDescriptorProto(
            RowType rowType, String messageName) {
        String sanitized = sanitizeMessageName(messageName);
        DescriptorProtos.DescriptorProto.Builder messageBuilder =
                DescriptorProtos.DescriptorProto.newBuilder().setName(sanitized);

        int fieldNumber = 1;
        for (RowType.RowField field : rowType.getFields()) {
            DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder =
                    DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setName(field.getName())
                            .setNumber(fieldNumber++)
                            .setType(toProtoFieldType(field.getType()))
                            .setLabel(
                                    DescriptorProtos.FieldDescriptorProto.Label
                                            .LABEL_OPTIONAL);
            messageBuilder.addField(fieldBuilder);
        }

        return DescriptorProtos.FileDescriptorProto.newBuilder()
                .setSyntax(PROTO_SYNTAX)
                .addMessageType(messageBuilder)
                .build();
    }

    /**
     * Sanitizes a schema name to be a valid Protobuf message name. Replaces non-alphanumeric
     * characters with underscores and ensures it starts with a letter.
     */
    static String sanitizeMessageName(String name) {
        String sanitized = name.replaceAll("[^a-zA-Z0-9_]", "_");
        if (!sanitized.isEmpty() && Character.isDigit(sanitized.charAt(0))) {
            sanitized = "M_" + sanitized;
        }
        if (sanitized.isEmpty()) {
            sanitized = "Message";
        }
        return sanitized;
    }

    private static String toProtoType(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return "bool";
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return "int32";
            case BIGINT:
                return "int64";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case CHAR:
            case VARCHAR:
                return "string";
            case BINARY:
            case VARBINARY:
                return "bytes";
            default:
                return "string";
        }
    }

    private static DescriptorProtos.FieldDescriptorProto.Type toProtoFieldType(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32;
            case BIGINT:
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
            case FLOAT:
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT;
            case DOUBLE:
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE;
            case CHAR:
            case VARCHAR:
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
            case BINARY:
            case VARBINARY:
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES;
            default:
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
        }
    }

    private ProtobufSchemaConverter() {}
}
