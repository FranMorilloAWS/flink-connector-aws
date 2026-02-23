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

package org.apache.flink.formats.json.glue.schema.registry;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import java.util.StringJoiner;

/**
 * Converts a Flink {@link RowType} to a JSON Schema (draft-07) string for registration with AWS
 * Glue Schema Registry.
 *
 * <p>This converter handles nullable types by using JSON Schema's type array syntax. For example, a
 * nullable string field becomes {@code "type": ["string", "null"]} instead of {@code "type":
 * "string"}.
 */
@Internal
public class JsonSchemaConverter {

    /**
     * Converts a Flink RowType to a JSON Schema string.
     *
     * @param rowType the Flink RowType
     * @return a JSON Schema string
     */
    public static String convertToJsonSchema(RowType rowType) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"properties\":{");
        StringJoiner props = new StringJoiner(",");
        for (RowType.RowField field : rowType.getFields()) {
            props.add("\"" + field.getName() + "\":" + convertType(field.getType()));
        }
        sb.append(props);
        sb.append("}}");
        return sb.toString();
    }

    private static String convertType(LogicalType type) {
        boolean nullable = type.isNullable();
        String baseType = getBaseType(type);

        if (nullable) {
            // For nullable types, we need to allow null in the type array
            // e.g., {"type": ["string", "null"]} or for objects: {"anyOf": [{"type": "object", ...}, {"type": "null"}]}
            if (baseType.startsWith("{\"type\":\"object\"") || baseType.startsWith("{\"type\":\"array\"")) {
                // For complex types (object, array), use anyOf to allow null
                return "{\"anyOf\":[" + baseType + ",{\"type\":\"null\"}]}";
            } else {
                // For simple types, extract the type name and make it an array with null
                // e.g., {"type":"string"} -> {"type":["string","null"]}
                String typeName = extractTypeName(baseType);
                return "{\"type\":[\"" + typeName + "\",\"null\"]}";
            }
        }
        return baseType;
    }

    private static String extractTypeName(String typeJson) {
        // Extract type name from {"type":"typename"}
        int start = typeJson.indexOf("\"type\":\"") + 8;
        int end = typeJson.indexOf("\"", start);
        return typeJson.substring(start, end);
    }

    private static String getBaseType(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return "{\"type\":\"boolean\"}";
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return "{\"type\":\"integer\"}";
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return "{\"type\":\"number\"}";
            case CHAR:
            case VARCHAR:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return "{\"type\":\"string\"}";
            case BINARY:
            case VARBINARY:
                return "{\"type\":\"string\"}";
            case ARRAY:
                LogicalType elementType = ((ArrayType) type).getElementType();
                return "{\"type\":\"array\",\"items\":" + convertType(elementType) + "}";
            case MAP:
                LogicalType valueType = ((MapType) type).getValueType();
                return "{\"type\":\"object\",\"additionalProperties\":" + convertType(valueType) + "}";
            case ROW:
                RowType rowType = (RowType) type;
                StringBuilder sb = new StringBuilder();
                sb.append("{\"type\":\"object\",\"properties\":{");
                StringJoiner joiner = new StringJoiner(",");
                for (RowType.RowField field : rowType.getFields()) {
                    joiner.add("\"" + field.getName() + "\":" + convertType(field.getType()));
                }
                sb.append(joiner);
                sb.append("}}");
                return sb.toString();
            case NULL:
                return "{\"type\":\"null\"}";
            default:
                return "{\"type\":\"string\"}";
        }
    }

    private JsonSchemaConverter() {}
}
