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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.IOException;
import java.util.Arrays;

/**
 * Deserialization schema that strips AWS Glue Schema Registry header bytes and delegates Protobuf
 * deserialization to produce Flink {@link RowData}.
 *
 * <p>The GSR header format is:
 *
 * <pre>
 * [1 byte: header version (0x03)] [1 byte: compression] [16 bytes: schema version UUID] [N bytes: Protobuf payload]
 * </pre>
 *
 * <p>Total header size: 18 bytes.
 */
@Internal
public class GsrProtobufRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    /** GSR header size: 1 (version) + 1 (compression) + 16 (UUID) = 18 bytes. */
    static final int GSR_HEADER_SIZE = 18;

    private final RowType rowType;
    private final TypeInformation<RowData> producedType;
    private final String schemaName;

    private transient Descriptors.Descriptor messageDescriptor;

    /**
     * Creates a new GSR Protobuf deserialization schema.
     *
     * @param rowType the Flink RowType describing the expected schema
     * @param producedType the type information for the produced RowData
     * @param schemaName the schema name used for building the Protobuf descriptor
     */
    public GsrProtobufRowDataDeserializationSchema(
            RowType rowType, TypeInformation<RowData> producedType, String schemaName) {
        this.rowType = rowType;
        this.producedType = producedType;
        this.schemaName = schemaName;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        if (messageDescriptor == null) {
            messageDescriptor = buildDescriptor();
        }
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        if (message.length < GSR_HEADER_SIZE) {
            throw new IOException(
                    "Invalid GSR-encoded data: expected at least "
                            + GSR_HEADER_SIZE
                            + " header bytes, but got "
                            + message.length);
        }

        // Strip the GSR header and parse the Protobuf payload
        byte[] protobufPayload = Arrays.copyOfRange(message, GSR_HEADER_SIZE, message.length);
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(messageDescriptor, protobufPayload);
        return ProtobufToRowDataConverter.convertToRowData(dynamicMessage, rowType);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }

    @VisibleForTesting
    void setMessageDescriptor(Descriptors.Descriptor descriptor) {
        this.messageDescriptor = descriptor;
    }

    private Descriptors.Descriptor buildDescriptor() {
        try {
            DescriptorProtos.FileDescriptorProto fileProto =
                    ProtobufSchemaConverter.buildFileDescriptorProto(rowType, schemaName);
            Descriptors.FileDescriptor fileDescriptor =
                    Descriptors.FileDescriptor.buildFrom(
                            fileProto, new Descriptors.FileDescriptor[] {});
            return fileDescriptor.findMessageTypeByName(
                    ProtobufSchemaConverter.sanitizeMessageName(schemaName));
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RuntimeException("Failed to build Protobuf descriptor from RowType", e);
        }
    }
}
