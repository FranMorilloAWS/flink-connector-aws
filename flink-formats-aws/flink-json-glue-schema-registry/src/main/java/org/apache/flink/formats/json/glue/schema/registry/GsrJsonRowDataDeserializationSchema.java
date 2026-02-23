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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.Arrays;

/**
 * Deserialization schema that strips AWS Glue Schema Registry header bytes and delegates JSON
 * deserialization to Flink's {@link JsonRowDataDeserializationSchema}.
 *
 * <p>The GSR header format is:
 *
 * <pre>
 * [1 byte: header version (0x03)] [1 byte: compression] [16 bytes: schema version UUID] [N bytes: JSON payload]
 * </pre>
 *
 * <p>Total header size: 18 bytes.
 */
@Internal
public class GsrJsonRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    /** GSR header size: 1 (version) + 1 (compression) + 16 (UUID) = 18 bytes. */
    static final int GSR_HEADER_SIZE = 18;

    private final JsonRowDataDeserializationSchema jsonDeserializer;
    private final TypeInformation<RowData> producedType;

    /**
     * Creates a new GSR JSON deserialization schema.
     *
     * @param jsonDeserializer the inner Flink JSON deserializer
     * @param producedType the type information for the produced RowData
     */
    public GsrJsonRowDataDeserializationSchema(
            JsonRowDataDeserializationSchema jsonDeserializer,
            TypeInformation<RowData> producedType) {
        this.jsonDeserializer = jsonDeserializer;
        this.producedType = producedType;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        jsonDeserializer.open(context);
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
        // Strip the GSR header and pass only the JSON payload to the inner deserializer
        byte[] jsonPayload = Arrays.copyOfRange(message, GSR_HEADER_SIZE, message.length);
        return jsonDeserializer.deserialize(jsonPayload);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }
}
