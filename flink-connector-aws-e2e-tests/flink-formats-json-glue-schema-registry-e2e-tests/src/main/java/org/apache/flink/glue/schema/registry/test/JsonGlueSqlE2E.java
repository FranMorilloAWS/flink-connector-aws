/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.glue.schema.registry.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * E2E application for the {@code json-glue} Flink SQL format factory.
 *
 * <p>Set the following environment variables before running:
 *
 * <ul>
 *   <li>{@code AWS_REGION} — e.g. us-east-1
 *   <li>{@code KINESIS_STREAM_ARN} — full ARN of an existing Kinesis stream
 *   <li>{@code GSR_REGISTRY_NAME} — existing Glue Schema Registry name
 *   <li>{@code GSR_SCHEMA_NAME} — schema name prefix for auto-registration
 * </ul>
 *
 * <p>Tests:
 *
 * <ul>
 *   <li>Test 1: Basic round-trip (STRING, INT, BOOLEAN)
 *   <li>Test 2: Complex types (nested ROW, ARRAY, MAP, DECIMAL, TIMESTAMP, nullable)
 *   <li>Test 3: Null handling for nullable fields
 *   <li>Test 4: Wide schema with many fields
 * </ul>
 */
public class JsonGlueSqlE2E {

    private static final Logger LOG = LoggerFactory.getLogger(JsonGlueSqlE2E.class);

    public static void main(String[] args) throws Exception {
        String awsRegion = requireEnv("AWS_REGION");
        String streamArn = requireEnv("KINESIS_STREAM_ARN");
        String registryName = requireEnv("GSR_REGISTRY_NAME");
        String schemaNamePrefix = requireEnv("GSR_SCHEMA_NAME");

        LOG.info("=== JSON-Glue SQL E2E Test ===");
        LOG.info("Region:       {}", awsRegion);
        LOG.info("Stream ARN:   {}", streamArn);
        LOG.info("Registry:     {}", registryName);
        LOG.info("Schema prefix:{}", schemaNamePrefix);

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv);

        // ================================================================
        // TEST 1: Basic round-trip (STRING, INT, BOOLEAN)
        // ================================================================
        LOG.info("=== Test 1: Basic round-trip (STRING, INT, BOOLEAN) ===");
        String basicSchemaName = schemaNamePrefix + "-basic";

        tEnv.executeSql(
                "CREATE TABLE kinesis_sink_basic ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  is_active BOOLEAN"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + basicSchemaName + "',"
                        + "  'json-glue.schema.autoRegistration' = 'true'"
                        + ")");

        LOG.info("Inserting 3 basic rows...");
        tEnv.executeSql(
                        "INSERT INTO kinesis_sink_basic VALUES "
                                + "('Alice', 30, true),"
                                + "('Bob', 25, false),"
                                + "('Charlie', 35, true)")
                .await(120, TimeUnit.SECONDS);

        LOG.info("INSERT complete. Now reading back...");

        tEnv.executeSql(
                "CREATE TABLE kinesis_source_basic ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  is_active BOOLEAN"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'source.init.position' = 'TRIM_HORIZON',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + basicSchemaName + "'"
                        + ")");

        TableResult result1 = tEnv.executeSql("SELECT * FROM kinesis_source_basic");
        List<Row> collected1 = new ArrayList<>();

        LOG.info("Collecting rows (timeout 90s)...");
        try (CloseableIterator<Row> iterator = result1.collect()) {
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
            while (collected1.size() < 3 && System.currentTimeMillis() < deadline) {
                if (iterator.hasNext()) {
                    Row row = iterator.next();
                    LOG.info("  Row {}: {}", collected1.size() + 1, row);
                    collected1.add(row);
                } else {
                    Thread.sleep(500);
                }
            }
        }

        LOG.info("Collected {} rows total", collected1.size());

        if (collected1.size() != 3) {
            LOG.error("FAIL test 1: expected 3 rows, got {}", collected1.size());
            System.exit(1);
        }

        List<String> names = new ArrayList<>();
        for (Row row : collected1) {
            names.add(row.getField(0).toString());
        }

        if (names.contains("Alice") && names.contains("Bob") && names.contains("Charlie")) {
            LOG.info("PASS test 1: all 3 rows round-tripped correctly via json-glue format");
        } else {
            LOG.error("FAIL test 1: unexpected names: {}", names);
            System.exit(1);
        }

        // ================================================================
        // TEST 2: Complex types (nested ROW, ARRAY, MAP, DECIMAL, TIMESTAMP)
        // ================================================================
        LOG.info("=== Test 2: Complex types (nested ROW, ARRAY, MAP, DECIMAL, TIMESTAMP) ===");
        String complexSchemaName = schemaNamePrefix + "-complex";

        tEnv.executeSql(
                "CREATE TABLE kinesis_sink_complex ("
                        + "  order_id STRING,"
                        + "  order_time TIMESTAMP(3),"
                        + "  total_amount DECIMAL(10, 2),"
                        + "  customer ROW<name STRING, email STRING, age INT>,"
                        + "  items ARRAY<ROW<product_name STRING, quantity INT, price DECIMAL(8, 2)>>,"
                        + "  tags ARRAY<STRING>,"
                        + "  metadata MAP<STRING, STRING>,"
                        + "  notes STRING"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + complexSchemaName + "',"
                        + "  'json-glue.schema.autoRegistration' = 'true'"
                        + ")");

        LOG.info("Inserting complex row with nested structures...");
        tEnv.executeSql(
                        "INSERT INTO kinesis_sink_complex VALUES ("
                                + "  'ORD-001',"
                                + "  TIMESTAMP '2024-01-15 10:30:00',"
                                + "  CAST(199.99 AS DECIMAL(10, 2)),"
                                + "  ROW('John Doe', 'john@example.com', 35),"
                                + "  ARRAY[ROW('Widget', 2, CAST(49.99 AS DECIMAL(8, 2))), "
                                + "        ROW('Gadget', 1, CAST(99.99 AS DECIMAL(8, 2)))],"
                                + "  ARRAY['priority', 'express'],"
                                + "  MAP['source', 'web', 'campaign', 'summer-sale'],"
                                + "  'Handle with care'"
                                + ")")
                .await(120, TimeUnit.SECONDS);

        LOG.info("INSERT with complex types complete. Now reading back...");

        tEnv.executeSql(
                "CREATE TABLE kinesis_source_complex ("
                        + "  order_id STRING,"
                        + "  order_time TIMESTAMP(3),"
                        + "  total_amount DECIMAL(10, 2),"
                        + "  customer ROW<name STRING, email STRING, age INT>,"
                        + "  items ARRAY<ROW<product_name STRING, quantity INT, price DECIMAL(8, 2)>>,"
                        + "  tags ARRAY<STRING>,"
                        + "  metadata MAP<STRING, STRING>,"
                        + "  notes STRING"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'source.init.position' = 'TRIM_HORIZON',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + complexSchemaName + "'"
                        + ")");

        TableResult result2 = tEnv.executeSql("SELECT * FROM kinesis_source_complex");
        boolean foundOrd001 = false;

        LOG.info("Collecting rows for test 2 (timeout 90s, looking for ORD-001)...");
        try (CloseableIterator<Row> iterator2 = result2.collect()) {
            long deadline2 = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
            while (!foundOrd001 && System.currentTimeMillis() < deadline2) {
                if (iterator2.hasNext()) {
                    Row row = iterator2.next();
                    Object orderIdField = row.getField(0);
                    if (orderIdField == null) {
                        LOG.warn("  Skipping row with null order_id: {}", row);
                        continue;
                    }
                    String orderId = orderIdField.toString();
                    LOG.info("  Row: {}", row);

                    if ("ORD-001".equals(orderId)) {
                        foundOrd001 = true;
                        // Verify nested customer
                        Row customer = (Row) row.getField(3);
                        if (!"John Doe".equals(customer.getField(0).toString())) {
                            LOG.error("FAIL: customer.name mismatch, expected 'John Doe', got '{}'",
                                    customer.getField(0));
                            System.exit(1);
                        }
                        // Verify DECIMAL
                        Object totalAmount = row.getField(2);
                        LOG.info("  total_amount: {} (type: {})", totalAmount,
                                totalAmount.getClass().getSimpleName());
                        // Verify ARRAY
                        Object items = row.getField(4);
                        LOG.info("  items: {} (type: {})", items, items.getClass().getSimpleName());
                        // Verify MAP
                        Object metadata = row.getField(6);
                        LOG.info("  metadata: {} (type: {})", metadata,
                                metadata.getClass().getSimpleName());
                        LOG.info("  ORD-001 verified: nested ROW, ARRAY, MAP, DECIMAL, TIMESTAMP OK");
                    }
                } else {
                    Thread.sleep(500);
                }
            }
        }

        if (foundOrd001) {
            LOG.info("PASS test 2: complex types round-trip works");
        } else {
            LOG.error("FAIL test 2: did not find ORD-001");
            System.exit(1);
        }

        // ================================================================
        // TEST 3: Null handling for nullable fields
        // ================================================================
        LOG.info("=== Test 3: Null handling for nullable fields ===");
        String nullableSchemaName = schemaNamePrefix + "-nullable";

        tEnv.executeSql(
                "CREATE TABLE kinesis_sink_nullable ("
                        + "  id STRING,"
                        + "  required_field STRING NOT NULL,"
                        + "  optional_string STRING,"
                        + "  optional_int INT,"
                        + "  optional_boolean BOOLEAN,"
                        + "  optional_decimal DECIMAL(10, 2),"
                        + "  optional_array ARRAY<STRING>,"
                        + "  optional_map MAP<STRING, STRING>,"
                        + "  optional_row ROW<a STRING, b INT>"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + nullableSchemaName + "',"
                        + "  'json-glue.schema.autoRegistration' = 'true'"
                        + ")");

        LOG.info("Inserting row with all fields populated...");
        tEnv.executeSql(
                        "INSERT INTO kinesis_sink_nullable VALUES ("
                                + "  'NULL-001',"
                                + "  'required',"
                                + "  'optional-value',"
                                + "  42,"
                                + "  true,"
                                + "  CAST(123.45 AS DECIMAL(10, 2)),"
                                + "  ARRAY['a', 'b'],"
                                + "  MAP['key', 'value'],"
                                + "  ROW('nested', 99)"
                                + ")")
                .await(120, TimeUnit.SECONDS);

        LOG.info("Inserting row with NULL optional fields...");
        tEnv.executeSql(
                        "INSERT INTO kinesis_sink_nullable VALUES ("
                                + "  'NULL-002',"
                                + "  'required-only',"
                                + "  CAST(NULL AS STRING),"
                                + "  CAST(NULL AS INT),"
                                + "  CAST(NULL AS BOOLEAN),"
                                + "  CAST(NULL AS DECIMAL(10, 2)),"
                                + "  CAST(NULL AS ARRAY<STRING>),"
                                + "  CAST(NULL AS MAP<STRING, STRING>),"
                                + "  CAST(NULL AS ROW<a STRING, b INT>)"
                                + ")")
                .await(120, TimeUnit.SECONDS);

        LOG.info("INSERT with nullable fields complete. Now reading back...");

        tEnv.executeSql(
                "CREATE TABLE kinesis_source_nullable ("
                        + "  id STRING,"
                        + "  required_field STRING,"
                        + "  optional_string STRING,"
                        + "  optional_int INT,"
                        + "  optional_boolean BOOLEAN,"
                        + "  optional_decimal DECIMAL(10, 2),"
                        + "  optional_array ARRAY<STRING>,"
                        + "  optional_map MAP<STRING, STRING>,"
                        + "  optional_row ROW<a STRING, b INT>"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'source.init.position' = 'TRIM_HORIZON',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + nullableSchemaName + "'"
                        + ")");

        TableResult result3 = tEnv.executeSql("SELECT * FROM kinesis_source_nullable");
        boolean foundNull001 = false;
        boolean foundNull002 = false;

        LOG.info("Collecting rows for test 3 (timeout 90s, looking for NULL-001 & NULL-002)...");
        try (CloseableIterator<Row> iterator3 = result3.collect()) {
            long deadline3 = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
            while (!(foundNull001 && foundNull002) && System.currentTimeMillis() < deadline3) {
                if (iterator3.hasNext()) {
                    Row row = iterator3.next();
                    Object idField = row.getField(0);
                    if (idField == null) {
                        continue;
                    }
                    String id = idField.toString();
                    LOG.info("  Row: {}", row);

                    if ("NULL-001".equals(id)) {
                        foundNull001 = true;
                        // Verify all optional fields are NOT null
                        if (row.getField(2) == null || row.getField(3) == null
                                || row.getField(4) == null || row.getField(5) == null
                                || row.getField(6) == null || row.getField(7) == null
                                || row.getField(8) == null) {
                            LOG.error("FAIL: NULL-001 should have all fields populated");
                            System.exit(1);
                        }
                        LOG.info("  NULL-001 verified: all optional fields populated");
                    }

                    if ("NULL-002".equals(id)) {
                        foundNull002 = true;
                        // Verify all optional fields ARE null
                        if (row.getField(2) != null || row.getField(3) != null
                                || row.getField(4) != null || row.getField(5) != null
                                || row.getField(6) != null || row.getField(7) != null
                                || row.getField(8) != null) {
                            LOG.error("FAIL: NULL-002 should have all optional fields null");
                            LOG.error("  optional_string: {}", row.getField(2));
                            LOG.error("  optional_int: {}", row.getField(3));
                            LOG.error("  optional_boolean: {}", row.getField(4));
                            LOG.error("  optional_decimal: {}", row.getField(5));
                            LOG.error("  optional_array: {}", row.getField(6));
                            LOG.error("  optional_map: {}", row.getField(7));
                            LOG.error("  optional_row: {}", row.getField(8));
                            System.exit(1);
                        }
                        LOG.info("  NULL-002 verified: all optional fields are null");
                    }
                } else {
                    Thread.sleep(500);
                }
            }
        }

        if (foundNull001 && foundNull002) {
            LOG.info("PASS test 3: null handling works correctly");
        } else {
            LOG.error("FAIL test 3: did not find NULL-001 ({}) and NULL-002 ({})",
                    foundNull001, foundNull002);
            System.exit(1);
        }

        // ================================================================
        // TEST 4: Wide schema with many fields (stress test)
        // ================================================================
        LOG.info("=== Test 4: Wide schema with many fields ===");
        String wideSchemaName = schemaNamePrefix + "-wide";

        tEnv.executeSql(
                "CREATE TABLE kinesis_sink_wide ("
                        + "  id STRING,"
                        + "  field_01 STRING, field_02 STRING, field_03 STRING, field_04 STRING, field_05 STRING,"
                        + "  field_06 INT, field_07 INT, field_08 INT, field_09 INT, field_10 INT,"
                        + "  field_11 BIGINT, field_12 BIGINT, field_13 BIGINT,"
                        + "  field_14 DOUBLE, field_15 DOUBLE, field_16 DOUBLE,"
                        + "  field_17 BOOLEAN, field_18 BOOLEAN, field_19 BOOLEAN, field_20 BOOLEAN"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + wideSchemaName + "',"
                        + "  'json-glue.schema.autoRegistration' = 'true'"
                        + ")");

        LOG.info("Inserting wide row with 21 fields...");
        tEnv.executeSql(
                        "INSERT INTO kinesis_sink_wide VALUES ("
                                + "  'WIDE-001',"
                                + "  'str1', 'str2', 'str3', 'str4', 'str5',"
                                + "  1, 2, 3, 4, 5,"
                                + "  100000000001, 100000000002, 100000000003,"
                                + "  1.1, 2.2, 3.3,"
                                + "  true, false, true, false"
                                + ")")
                .await(120, TimeUnit.SECONDS);

        LOG.info("INSERT with wide schema complete. Now reading back...");

        tEnv.executeSql(
                "CREATE TABLE kinesis_source_wide ("
                        + "  id STRING,"
                        + "  field_01 STRING, field_02 STRING, field_03 STRING, field_04 STRING, field_05 STRING,"
                        + "  field_06 INT, field_07 INT, field_08 INT, field_09 INT, field_10 INT,"
                        + "  field_11 BIGINT, field_12 BIGINT, field_13 BIGINT,"
                        + "  field_14 DOUBLE, field_15 DOUBLE, field_16 DOUBLE,"
                        + "  field_17 BOOLEAN, field_18 BOOLEAN, field_19 BOOLEAN, field_20 BOOLEAN"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'source.init.position' = 'TRIM_HORIZON',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + wideSchemaName + "'"
                        + ")");

        TableResult result4 = tEnv.executeSql("SELECT * FROM kinesis_source_wide");
        boolean foundWide001 = false;

        LOG.info("Collecting rows for test 4 (timeout 90s, looking for WIDE-001)...");
        try (CloseableIterator<Row> iterator4 = result4.collect()) {
            long deadline4 = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
            while (!foundWide001 && System.currentTimeMillis() < deadline4) {
                if (iterator4.hasNext()) {
                    Row row = iterator4.next();
                    Object idField = row.getField(0);
                    if (idField == null) {
                        continue;
                    }
                    String id = idField.toString();
                    LOG.info("  Row: {}", row);

                    if ("WIDE-001".equals(id)) {
                        foundWide001 = true;
                        // Spot-check a few fields
                        if (!"str1".equals(row.getField(1).toString())) {
                            LOG.error("FAIL: field_01 mismatch");
                            System.exit(1);
                        }
                        if (!Integer.valueOf(5).equals(row.getField(10))) {
                            LOG.error("FAIL: field_10 mismatch, expected 5, got {}", row.getField(10));
                            System.exit(1);
                        }
                        if (!Boolean.FALSE.equals(row.getField(20))) {
                            LOG.error("FAIL: field_20 mismatch, expected false, got {}", row.getField(20));
                            System.exit(1);
                        }
                        LOG.info("  WIDE-001 verified: all 21 fields round-tripped correctly");
                    }
                } else {
                    Thread.sleep(500);
                }
            }
        }

        if (foundWide001) {
            LOG.info("PASS test 4: wide schema round-trip works");
        } else {
            LOG.error("FAIL test 4: did not find WIDE-001");
            System.exit(1);
        }

        // ================================================================
        // TEST 5: DATE and TIME types
        // ================================================================
        LOG.info("=== Test 5: DATE and TIME types ===");
        String dateTimeSchemaName = schemaNamePrefix + "-datetime";

        tEnv.executeSql(
                "CREATE TABLE kinesis_sink_datetime ("
                        + "  id STRING,"
                        + "  event_date DATE,"
                        + "  event_time TIME(0),"
                        + "  event_timestamp TIMESTAMP(3),"
                        + "  event_timestamp_ltz TIMESTAMP_LTZ(3)"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + dateTimeSchemaName + "',"
                        + "  'json-glue.schema.autoRegistration' = 'true'"
                        + ")");

        LOG.info("Inserting row with DATE/TIME types...");
        tEnv.executeSql(
                        "INSERT INTO kinesis_sink_datetime VALUES ("
                                + "  'DT-001',"
                                + "  DATE '2024-06-15',"
                                + "  TIME '14:30:00',"
                                + "  TIMESTAMP '2024-06-15 14:30:00.123',"
                                + "  TO_TIMESTAMP_LTZ(1718458200123, 3)"
                                + ")")
                .await(120, TimeUnit.SECONDS);

        LOG.info("INSERT with DATE/TIME types complete. Now reading back...");

        tEnv.executeSql(
                "CREATE TABLE kinesis_source_datetime ("
                        + "  id STRING,"
                        + "  event_date DATE,"
                        + "  event_time TIME(0),"
                        + "  event_timestamp TIMESTAMP(3),"
                        + "  event_timestamp_ltz TIMESTAMP_LTZ(3)"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'source.init.position' = 'TRIM_HORIZON',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + dateTimeSchemaName + "'"
                        + ")");

        TableResult result5 = tEnv.executeSql("SELECT * FROM kinesis_source_datetime");
        boolean foundDt001 = false;

        LOG.info("Collecting rows for test 5 (timeout 90s, looking for DT-001)...");
        try (CloseableIterator<Row> iterator5 = result5.collect()) {
            long deadline5 = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
            while (!foundDt001 && System.currentTimeMillis() < deadline5) {
                if (iterator5.hasNext()) {
                    Row row = iterator5.next();
                    Object idField = row.getField(0);
                    if (idField == null) {
                        continue;
                    }
                    String id = idField.toString();
                    LOG.info("  Row: {}", row);

                    if ("DT-001".equals(id)) {
                        foundDt001 = true;
                        LOG.info("  event_date: {} (type: {})", row.getField(1),
                                row.getField(1).getClass().getSimpleName());
                        LOG.info("  event_time: {} (type: {})", row.getField(2),
                                row.getField(2).getClass().getSimpleName());
                        LOG.info("  event_timestamp: {} (type: {})", row.getField(3),
                                row.getField(3).getClass().getSimpleName());
                        LOG.info("  event_timestamp_ltz: {} (type: {})", row.getField(4),
                                row.getField(4).getClass().getSimpleName());
                        LOG.info("  DT-001 verified: DATE/TIME types round-tripped");
                    }
                } else {
                    Thread.sleep(500);
                }
            }
        }

        if (foundDt001) {
            LOG.info("PASS test 5: DATE/TIME types round-trip works");
        } else {
            LOG.error("FAIL test 5: did not find DT-001");
            System.exit(1);
        }

        // ================================================================
        // TEST 6: Write to existing schema WITHOUT auto-registration
        // ================================================================
        LOG.info("=== Test 6: Write to existing schema WITHOUT auto-registration ===");
        // Reuse the basic schema that was already registered by Test 1
        // This tests that we can write to an existing schema without enabling autoRegistration

        tEnv.executeSql(
                "CREATE TABLE kinesis_sink_no_autoreg ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  is_active BOOLEAN"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + basicSchemaName + "'"
                        // NOTE: No 'json-glue.schema.autoRegistration' = 'true' here!
                        + ")");

        LOG.info("Inserting rows using existing schema (no auto-registration)...");
        tEnv.executeSql(
                        "INSERT INTO kinesis_sink_no_autoreg VALUES "
                                + "('NoAutoReg-Alice', 40, true),"
                                + "('NoAutoReg-Bob', 45, false)")
                .await(120, TimeUnit.SECONDS);

        LOG.info("INSERT without auto-registration complete. Now reading back...");

        tEnv.executeSql(
                "CREATE TABLE kinesis_source_no_autoreg ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  is_active BOOLEAN"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'source.init.position' = 'TRIM_HORIZON',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + basicSchemaName + "'"
                        + ")");

        TableResult result6 = tEnv.executeSql("SELECT * FROM kinesis_source_no_autoreg");
        boolean foundNoAutoRegAlice = false;
        boolean foundNoAutoRegBob = false;

        LOG.info("Collecting rows for test 6 (timeout 90s, looking for NoAutoReg-Alice & NoAutoReg-Bob)...");
        try (CloseableIterator<Row> iterator6 = result6.collect()) {
            long deadline6 = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
            while (!(foundNoAutoRegAlice && foundNoAutoRegBob) && System.currentTimeMillis() < deadline6) {
                if (iterator6.hasNext()) {
                    Row row = iterator6.next();
                    Object nameField = row.getField(0);
                    if (nameField == null) {
                        continue;
                    }
                    String name = nameField.toString();
                    LOG.info("  Row: {}", row);

                    if ("NoAutoReg-Alice".equals(name)) {
                        foundNoAutoRegAlice = true;
                        if (!Integer.valueOf(40).equals(row.getField(1))) {
                            LOG.error("FAIL: NoAutoReg-Alice age mismatch, expected 40, got {}", row.getField(1));
                            System.exit(1);
                        }
                        LOG.info("  NoAutoReg-Alice verified");
                    }

                    if ("NoAutoReg-Bob".equals(name)) {
                        foundNoAutoRegBob = true;
                        if (!Integer.valueOf(45).equals(row.getField(1))) {
                            LOG.error("FAIL: NoAutoReg-Bob age mismatch, expected 45, got {}", row.getField(1));
                            System.exit(1);
                        }
                        LOG.info("  NoAutoReg-Bob verified");
                    }
                } else {
                    Thread.sleep(500);
                }
            }
        }

        if (foundNoAutoRegAlice && foundNoAutoRegBob) {
            LOG.info("PASS test 6: writing to existing schema without auto-registration works");
        } else {
            LOG.error("FAIL test 6: did not find NoAutoReg-Alice ({}) and NoAutoReg-Bob ({})",
                    foundNoAutoRegAlice, foundNoAutoRegBob);
            System.exit(1);
        }

        // ================================================================
        // TEST 7: Schema compatibility — BACKWARD
        //
        // BACKWARD compatibility means new schema can read data written
        // with the old schema. Removing a REQUIRED field is NOT allowed
        // because a new reader would be missing a field that old data has.
        //
        // Strategy: use NOT NULL fields so Flink generates JSON Schema
        // with "required" array entries. Then removing a required field
        // triggers a true BACKWARD violation.
        //
        // IMPORTANT: delete the 'flink-json-glue-e2e-schema-compat-backward'
        // schema from GSR before re-running this test.
        // ================================================================
        LOG.info("=== Test 7: Schema compatibility — BACKWARD ===");
        String backwardSchemaName = schemaNamePrefix + "-compat-backward";

        // Step 1: v1 schema — all fields NOT NULL (required in JSON Schema)
        tEnv.executeSql(
                "CREATE TABLE compat_bw_sink_v1 ("
                        + "  user_name STRING NOT NULL,"
                        + "  age INT NOT NULL,"
                        + "  city STRING NOT NULL"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + backwardSchemaName + "',"
                        + "  'json-glue.schema.autoRegistration' = 'true',"
                        + "  'json-glue.schema.compatibility' = 'BACKWARD'"
                        + ")");

        LOG.info("Test 7 step 1: Writing v1 data (3 NOT NULL fields)...");
        tEnv.executeSql(
                        "INSERT INTO compat_bw_sink_v1 VALUES ('Alice', 30, 'Seattle')")
                .await(120, TimeUnit.SECONDS);
        LOG.info("Test 7 step 1: v1 write succeeded");

        // Step 2: v2 schema — add a nullable field (backward-compatible)
        tEnv.executeSql(
                "CREATE TABLE compat_bw_sink_v2 ("
                        + "  user_name STRING NOT NULL,"
                        + "  age INT NOT NULL,"
                        + "  city STRING NOT NULL,"
                        + "  email STRING"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + backwardSchemaName + "',"
                        + "  'json-glue.schema.autoRegistration' = 'true',"
                        + "  'json-glue.schema.compatibility' = 'BACKWARD'"
                        + ")");

        LOG.info("Test 7 step 2: Writing v2 data (added nullable email)...");
        tEnv.executeSql(
                        "INSERT INTO compat_bw_sink_v2 VALUES ('Bob', 25, 'Portland', 'bob@example.com')")
                .await(120, TimeUnit.SECONDS);
        LOG.info("Test 7 step 2: v2 write succeeded (adding nullable field is backward-compatible)");

        // Step 3: v3 schema — remove required 'city' field
        // This is a true BACKWARD violation: old data has a required 'city'
        // property in the JSON Schema "required" array, but the new schema
        // doesn't include it. The new reader cannot handle old data.
        tEnv.executeSql(
                "CREATE TABLE compat_bw_sink_v3 ("
                        + "  user_name STRING NOT NULL,"
                        + "  age INT NOT NULL"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + backwardSchemaName + "',"
                        + "  'json-glue.schema.autoRegistration' = 'true',"
                        + "  'json-glue.schema.compatibility' = 'BACKWARD'"
                        + ")");

        LOG.info("Test 7 step 3: Writing v3 data (removed required 'city' — BACKWARD violation)...");
        try {
            tEnv.executeSql(
                            "INSERT INTO compat_bw_sink_v3 VALUES ('Charlie', 35)")
                    .await(120, TimeUnit.SECONDS);
            LOG.error("FAIL test 7 step 3: expected schema compatibility rejection but write succeeded");
            System.exit(1);
        } catch (Exception e) {
            LOG.info("Test 7 step 3: Write correctly rejected by GSR: {}", e.getMessage());
            LOG.info("PASS test 7: BACKWARD compatibility enforced correctly");
        }

        // ================================================================
        // TEST 8: Schema compatibility — NONE (no validation)
        // ================================================================
        LOG.info("=== Test 8: Schema compatibility — NONE ===");
        String noneSchemaName = schemaNamePrefix + "-compat-none";

        // v1: 3 fields
        tEnv.executeSql(
                "CREATE TABLE compat_none_sink_v1 ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  city STRING"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + noneSchemaName + "',"
                        + "  'json-glue.schema.autoRegistration' = 'true',"
                        + "  'json-glue.schema.compatibility' = 'NONE'"
                        + ")");

        LOG.info("Test 8 step 1: Writing v1 data with NONE compat...");
        tEnv.executeSql(
                        "INSERT INTO compat_none_sink_v1 VALUES ('Dave', 40, 'Denver')")
                .await(120, TimeUnit.SECONDS);

        // v2: completely different schema (2 fields, removed city)
        tEnv.executeSql(
                "CREATE TABLE compat_none_sink_v2 ("
                        + "  user_name STRING,"
                        + "  age INT"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + noneSchemaName + "',"
                        + "  'json-glue.schema.autoRegistration' = 'true',"
                        + "  'json-glue.schema.compatibility' = 'NONE'"
                        + ")");

        LOG.info("Test 8 step 2: Writing v2 data (incompatible change, should succeed with NONE)...");
        tEnv.executeSql(
                        "INSERT INTO compat_none_sink_v2 VALUES ('Eve', 28)")
                .await(120, TimeUnit.SECONDS);
        LOG.info("PASS test 8: NONE compatibility allows any schema evolution");

        // ================================================================
        // TEST 9: Schema compatibility — FULL
        //
        // FULL = both BACKWARD and FORWARD. Adding an optional property
        // is the canonical safe evolution.
        // ================================================================
        LOG.info("=== Test 9: Schema compatibility — FULL ===");
        String fullSchemaName = schemaNamePrefix + "-compat-full";

        // v1: 3 fields
        tEnv.executeSql(
                "CREATE TABLE compat_full_sink_v1 ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  city STRING"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + fullSchemaName + "',"
                        + "  'json-glue.schema.autoRegistration' = 'true',"
                        + "  'json-glue.schema.compatibility' = 'FULL'"
                        + ")");

        LOG.info("Test 9 step 1: Writing v1 data with FULL compat...");
        tEnv.executeSql(
                        "INSERT INTO compat_full_sink_v1 VALUES ('Frank', 45, 'Chicago')")
                .await(120, TimeUnit.SECONDS);

        // v2: add optional field
        tEnv.executeSql(
                "CREATE TABLE compat_full_sink_v2 ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  city STRING,"
                        + "  email STRING"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '" + streamArn + "',"
                        + "  'aws.region' = '" + awsRegion + "',"
                        + "  'format' = 'json-glue',"
                        + "  'json-glue.aws.region' = '" + awsRegion + "',"
                        + "  'json-glue.registry.name' = '" + registryName + "',"
                        + "  'json-glue.schema.name' = '" + fullSchemaName + "',"
                        + "  'json-glue.schema.autoRegistration' = 'true',"
                        + "  'json-glue.schema.compatibility' = 'FULL'"
                        + ")");

        LOG.info("Test 9 step 2: Writing v2 data (added optional email)...");
        tEnv.executeSql(
                        "INSERT INTO compat_full_sink_v2 VALUES ('Grace', 32, 'Boston', 'grace@example.com')")
                .await(120, TimeUnit.SECONDS);
        LOG.info("PASS test 9: FULL compatibility allows adding optional properties");

        LOG.info("=== All JSON-Glue E2E Tests Passed ===");
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            System.err.println("ERROR: environment variable " + name + " is required");
            System.exit(1);
        }
        return value;
    }
}
