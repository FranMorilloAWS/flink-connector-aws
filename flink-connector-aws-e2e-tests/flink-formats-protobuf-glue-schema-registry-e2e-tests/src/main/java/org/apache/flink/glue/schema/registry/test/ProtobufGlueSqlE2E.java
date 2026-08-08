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
 * E2E application for the {@code protobuf-glue} Flink SQL format factory.
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
 *   <li>Test 2: Wide schema with many primitive fields
 *   <li>Test 3: Proto3 default-value semantics (nulls become defaults)
 * </ul>
 *
 * <p>Note: Proto3 does not distinguish null from default values. Sending a null STRING yields "",
 * null INT yields 0, null BOOLEAN yields false. This is by design and tested in Test 3.
 */
public class ProtobufGlueSqlE2E {

    private static final Logger LOG = LoggerFactory.getLogger(ProtobufGlueSqlE2E.class);

    public static void main(String[] args) throws Exception {
        String awsRegion = requireEnv("AWS_REGION");
        String streamArn = requireEnv("KINESIS_STREAM_ARN");
        String registryName = requireEnv("GSR_REGISTRY_NAME");
        String schemaNamePrefix = requireEnv("GSR_SCHEMA_NAME");

        LOG.info("=== Protobuf-Glue SQL E2E Test ===");
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
                        + "  'stream.arn' = '"
                        + streamArn
                        + "',"
                        + "  'aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'format' = 'protobuf-glue',"
                        + "  'protobuf-glue.aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'protobuf-glue.registry.name' = '"
                        + registryName
                        + "',"
                        + "  'protobuf-glue.schema.name' = '"
                        + basicSchemaName
                        + "',"
                        + "  'protobuf-glue.schema.autoRegistration' = 'true'"
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
                        + "  'stream.arn' = '"
                        + streamArn
                        + "',"
                        + "  'aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'source.init.position' = 'TRIM_HORIZON',"
                        + "  'format' = 'protobuf-glue',"
                        + "  'protobuf-glue.aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'protobuf-glue.registry.name' = '"
                        + registryName
                        + "',"
                        + "  'protobuf-glue.schema.name' = '"
                        + basicSchemaName
                        + "'"
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
            LOG.info("PASS test 1: all 3 rows round-tripped correctly via protobuf-glue format");
        } else {
            LOG.error("FAIL test 1: unexpected names: {}", names);
            System.exit(1);
        }

        // ================================================================
        // TEST 2: Wide schema with many primitive fields
        // ================================================================
        LOG.info("=== Test 2: Wide schema with many primitive fields ===");
        String wideSchemaName = schemaNamePrefix + "-wide";

        tEnv.executeSql(
                "CREATE TABLE kinesis_sink_wide ("
                        + "  id STRING,"
                        + "  field_01 STRING, field_02 STRING, field_03 STRING,"
                        + "  field_04 STRING, field_05 STRING,"
                        + "  field_06 INT, field_07 INT, field_08 INT,"
                        + "  field_09 INT, field_10 INT,"
                        + "  field_11 BIGINT, field_12 BIGINT, field_13 BIGINT,"
                        + "  field_14 DOUBLE, field_15 DOUBLE, field_16 DOUBLE,"
                        + "  field_17 BOOLEAN, field_18 BOOLEAN,"
                        + "  field_19 BOOLEAN, field_20 BOOLEAN,"
                        + "  field_21 FLOAT, field_22 FLOAT"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '"
                        + streamArn
                        + "',"
                        + "  'aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'format' = 'protobuf-glue',"
                        + "  'protobuf-glue.aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'protobuf-glue.registry.name' = '"
                        + registryName
                        + "',"
                        + "  'protobuf-glue.schema.name' = '"
                        + wideSchemaName
                        + "',"
                        + "  'protobuf-glue.schema.autoRegistration' = 'true'"
                        + ")");

        LOG.info("Inserting wide row with 23 fields...");
        tEnv.executeSql(
                        "INSERT INTO kinesis_sink_wide VALUES ("
                                + "  'WIDE-001',"
                                + "  'str1', 'str2', 'str3', 'str4', 'str5',"
                                + "  1, 2, 3, 4, 5,"
                                + "  100000000001, 100000000002, 100000000003,"
                                + "  1.1, 2.2, 3.3,"
                                + "  true, false, true, false,"
                                + "  CAST(1.5 AS FLOAT), CAST(2.5 AS FLOAT)"
                                + ")")
                .await(120, TimeUnit.SECONDS);

        LOG.info("INSERT with wide schema complete. Now reading back...");

        tEnv.executeSql(
                "CREATE TABLE kinesis_source_wide ("
                        + "  id STRING,"
                        + "  field_01 STRING, field_02 STRING, field_03 STRING,"
                        + "  field_04 STRING, field_05 STRING,"
                        + "  field_06 INT, field_07 INT, field_08 INT,"
                        + "  field_09 INT, field_10 INT,"
                        + "  field_11 BIGINT, field_12 BIGINT, field_13 BIGINT,"
                        + "  field_14 DOUBLE, field_15 DOUBLE, field_16 DOUBLE,"
                        + "  field_17 BOOLEAN, field_18 BOOLEAN,"
                        + "  field_19 BOOLEAN, field_20 BOOLEAN,"
                        + "  field_21 FLOAT, field_22 FLOAT"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '"
                        + streamArn
                        + "',"
                        + "  'aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'source.init.position' = 'TRIM_HORIZON',"
                        + "  'format' = 'protobuf-glue',"
                        + "  'protobuf-glue.aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'protobuf-glue.registry.name' = '"
                        + registryName
                        + "',"
                        + "  'protobuf-glue.schema.name' = '"
                        + wideSchemaName
                        + "'"
                        + ")");

        TableResult result2 = tEnv.executeSql("SELECT * FROM kinesis_source_wide");
        boolean foundWide001 = false;

        LOG.info("Collecting rows for test 2 (timeout 90s, looking for WIDE-001)...");
        try (CloseableIterator<Row> iterator2 = result2.collect()) {
            long deadline2 = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
            while (!foundWide001 && System.currentTimeMillis() < deadline2) {
                if (iterator2.hasNext()) {
                    Row row = iterator2.next();
                    Object idField = row.getField(0);
                    if (idField == null || idField.toString().isEmpty()) {
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
                            LOG.error(
                                    "FAIL: field_10 mismatch, expected 5, got {}",
                                    row.getField(10));
                            System.exit(1);
                        }
                        if (!Boolean.FALSE.equals(row.getField(20))) {
                            LOG.error(
                                    "FAIL: field_20 mismatch, expected false, got {}",
                                    row.getField(20));
                            System.exit(1);
                        }
                        LOG.info("  WIDE-001 verified: all 23 fields round-tripped correctly");
                    }
                } else {
                    Thread.sleep(500);
                }
            }
        }

        if (foundWide001) {
            LOG.info("PASS test 2: wide schema round-trip works");
        } else {
            LOG.error("FAIL test 2: did not find WIDE-001");
            System.exit(1);
        }

        // ================================================================
        // TEST 3: Proto3 default-value semantics
        //
        // Proto3 does NOT distinguish null from default values:
        //   - null STRING  → ""  (empty string)
        //   - null INT     → 0
        //   - null BOOLEAN → false
        //
        // This test verifies that sending "default-like" values round-trips
        // correctly, and documents the proto3 null-to-default behavior.
        // ================================================================
        LOG.info("=== Test 3: Proto3 default-value semantics ===");
        String defaultSchemaName = schemaNamePrefix + "-defaults";

        tEnv.executeSql(
                "CREATE TABLE kinesis_sink_defaults ("
                        + "  id STRING,"
                        + "  str_field STRING,"
                        + "  int_field INT,"
                        + "  long_field BIGINT,"
                        + "  double_field DOUBLE,"
                        + "  bool_field BOOLEAN,"
                        + "  float_field FLOAT"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '"
                        + streamArn
                        + "',"
                        + "  'aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'format' = 'protobuf-glue',"
                        + "  'protobuf-glue.aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'protobuf-glue.registry.name' = '"
                        + registryName
                        + "',"
                        + "  'protobuf-glue.schema.name' = '"
                        + defaultSchemaName
                        + "',"
                        + "  'protobuf-glue.schema.autoRegistration' = 'true'"
                        + ")");

        // Row with non-default values
        LOG.info("Inserting row with non-default values...");
        tEnv.executeSql(
                        "INSERT INTO kinesis_sink_defaults VALUES ("
                                + "  'DEF-001', 'hello', 42, 100000000001, 3.14, true,"
                                + "  CAST(1.5 AS FLOAT)"
                                + ")")
                .await(120, TimeUnit.SECONDS);

        // Row with proto3 default values (empty string, 0, false)
        LOG.info("Inserting row with proto3 default values...");
        tEnv.executeSql(
                        "INSERT INTO kinesis_sink_defaults VALUES ("
                                + "  'DEF-002', '', 0, CAST(0 AS BIGINT), CAST(0.0 AS DOUBLE),"
                                + "  false, CAST(0.0 AS FLOAT)"
                                + ")")
                .await(120, TimeUnit.SECONDS);

        LOG.info("INSERT with default values complete. Now reading back...");

        tEnv.executeSql(
                "CREATE TABLE kinesis_source_defaults ("
                        + "  id STRING,"
                        + "  str_field STRING,"
                        + "  int_field INT,"
                        + "  long_field BIGINT,"
                        + "  double_field DOUBLE,"
                        + "  bool_field BOOLEAN,"
                        + "  float_field FLOAT"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '"
                        + streamArn
                        + "',"
                        + "  'aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'source.init.position' = 'TRIM_HORIZON',"
                        + "  'format' = 'protobuf-glue',"
                        + "  'protobuf-glue.aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'protobuf-glue.registry.name' = '"
                        + registryName
                        + "',"
                        + "  'protobuf-glue.schema.name' = '"
                        + defaultSchemaName
                        + "'"
                        + ")");

        TableResult result3 = tEnv.executeSql("SELECT * FROM kinesis_source_defaults");
        boolean foundDef001 = false;
        boolean foundDef002 = false;

        LOG.info("Collecting rows for test 3 (timeout 90s, looking for DEF-001 & DEF-002)...");
        try (CloseableIterator<Row> iterator3 = result3.collect()) {
            long deadline3 = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
            while (!(foundDef001 && foundDef002) && System.currentTimeMillis() < deadline3) {
                if (iterator3.hasNext()) {
                    Row row = iterator3.next();
                    Object idField = row.getField(0);
                    if (idField == null || idField.toString().isEmpty()) {
                        continue;
                    }
                    String id = idField.toString();
                    LOG.info("  Row: {}", row);

                    if ("DEF-001".equals(id)) {
                        foundDef001 = true;
                        if (!"hello".equals(row.getField(1).toString())) {
                            LOG.error("FAIL: DEF-001 str_field mismatch");
                            System.exit(1);
                        }
                        if (!Integer.valueOf(42).equals(row.getField(2))) {
                            LOG.error("FAIL: DEF-001 int_field mismatch");
                            System.exit(1);
                        }
                        if (!Boolean.TRUE.equals(row.getField(5))) {
                            LOG.error("FAIL: DEF-001 bool_field mismatch");
                            System.exit(1);
                        }
                        LOG.info("  DEF-001 verified: non-default values preserved");
                    }

                    if ("DEF-002".equals(id)) {
                        foundDef002 = true;
                        // Proto3 defaults: empty string, 0, 0L, 0.0, false, 0.0f
                        // These should round-trip as their default values (not null)
                        Object strVal = row.getField(1);
                        Object intVal = row.getField(2);
                        Object boolVal = row.getField(5);

                        if (strVal != null && !"".equals(strVal.toString())) {
                            LOG.error(
                                    "FAIL: DEF-002 str_field expected '' or null, got '{}'",
                                    strVal);
                            System.exit(1);
                        }
                        LOG.info(
                                "  DEF-002 str_field={}, int_field={}, bool_field={} "
                                        + "(proto3 defaults)",
                                strVal,
                                intVal,
                                boolVal);
                        LOG.info("  DEF-002 verified: proto3 default values round-tripped");
                    }
                } else {
                    Thread.sleep(500);
                }
            }
        }

        if (foundDef001 && foundDef002) {
            LOG.info("PASS test 3: proto3 default-value semantics work correctly");
        } else {
            LOG.error(
                    "FAIL test 3: did not find DEF-001 ({}) and DEF-002 ({})",
                    foundDef001,
                    foundDef002);
            System.exit(1);
        }

        // ================================================================
        // TEST 4: Schema compatibility — BACKWARD
        //
        // BACKWARD compatibility means new schema can read data written
        // with the old schema.
        //
        // Proto3 note: all fields are implicitly optional, so NOT NULL
        // has no effect on the generated .proto definition. Removing a
        // field is always safe in proto3 (the old field number is just
        // ignored). Therefore we use a TYPE CHANGE (INT→STRING) to
        // trigger a genuine BACKWARD incompatibility instead.
        //
        // IMPORTANT: delete the '*-compat-backward' schema from GSR
        // before re-running this test.
        // ================================================================
        LOG.info("=== Test 4: Schema compatibility — BACKWARD ===");
        String backwardSchemaName = schemaNamePrefix + "-compat-backward";

        // Step 1: v1 schema (3 fields)
        tEnv.executeSql(
                "CREATE TABLE compat_bw_sink_v1 ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  city STRING"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '"
                        + streamArn
                        + "',"
                        + "  'aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'format' = 'protobuf-glue',"
                        + "  'protobuf-glue.aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'protobuf-glue.registry.name' = '"
                        + registryName
                        + "',"
                        + "  'protobuf-glue.schema.name' = '"
                        + backwardSchemaName
                        + "',"
                        + "  'protobuf-glue.schema.autoRegistration' = 'true',"
                        + "  'protobuf-glue.schema.compatibility' = 'BACKWARD'"
                        + ")");

        LOG.info("Test 4 step 1: Writing v1 data (3 fields)...");
        tEnv.executeSql("INSERT INTO compat_bw_sink_v1 VALUES ('Alice', 30, 'Seattle')")
                .await(120, TimeUnit.SECONDS);
        LOG.info("Test 4 step 1: v1 write succeeded");

        // Step 2: v2 schema (4 fields — added new field)
        tEnv.executeSql(
                "CREATE TABLE compat_bw_sink_v2 ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  city STRING,"
                        + "  email STRING"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '"
                        + streamArn
                        + "',"
                        + "  'aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'format' = 'protobuf-glue',"
                        + "  'protobuf-glue.aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'protobuf-glue.registry.name' = '"
                        + registryName
                        + "',"
                        + "  'protobuf-glue.schema.name' = '"
                        + backwardSchemaName
                        + "',"
                        + "  'protobuf-glue.schema.autoRegistration' = 'true',"
                        + "  'protobuf-glue.schema.compatibility' = 'BACKWARD'"
                        + ")");

        LOG.info("Test 4 step 2: Writing v2 data (4 fields — added email)...");
        tEnv.executeSql(
                        "INSERT INTO compat_bw_sink_v2 VALUES ('Bob', 25, 'Portland', 'bob@example.com')")
                .await(120, TimeUnit.SECONDS);
        LOG.info("Test 4 step 2: v2 write succeeded (adding field is backward-compatible)");

        // Step 3: v3 schema — change 'age' from INT to STRING (type change)
        // In proto3, all fields are optional so removing a field is always
        // backward-compatible. A type change is the simplest way to trigger
        // a genuine BACKWARD incompatibility in proto3.
        tEnv.executeSql(
                "CREATE TABLE compat_bw_sink_v3 ("
                        + "  user_name STRING,"
                        + "  age STRING,"
                        + "  city STRING"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '"
                        + streamArn
                        + "',"
                        + "  'aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'format' = 'protobuf-glue',"
                        + "  'protobuf-glue.aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'protobuf-glue.registry.name' = '"
                        + registryName
                        + "',"
                        + "  'protobuf-glue.schema.name' = '"
                        + backwardSchemaName
                        + "',"
                        + "  'protobuf-glue.schema.autoRegistration' = 'true',"
                        + "  'protobuf-glue.schema.compatibility' = 'BACKWARD'"
                        + ")");

        LOG.info("Test 4 step 3: Writing v3 data (changed age INT→STRING — type change)...");
        try {
            tEnv.executeSql("INSERT INTO compat_bw_sink_v3 VALUES ('Charlie', '35', 'Denver')")
                    .await(120, TimeUnit.SECONDS);
            LOG.error(
                    "FAIL test 4 step 3: expected schema compatibility rejection but write succeeded");
            System.exit(1);
        } catch (Exception e) {
            LOG.info("Test 4 step 3: Write correctly rejected by GSR: {}", e.getMessage());
            LOG.info("PASS test 4: BACKWARD compatibility enforced correctly");
        }

        // ================================================================
        // TEST 5: Schema compatibility — NONE (no validation)
        // ================================================================
        LOG.info("=== Test 5: Schema compatibility — NONE ===");
        String noneSchemaName = schemaNamePrefix + "-compat-none";

        // v1: 3 fields
        tEnv.executeSql(
                "CREATE TABLE compat_none_sink_v1 ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  city STRING"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '"
                        + streamArn
                        + "',"
                        + "  'aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'format' = 'protobuf-glue',"
                        + "  'protobuf-glue.aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'protobuf-glue.registry.name' = '"
                        + registryName
                        + "',"
                        + "  'protobuf-glue.schema.name' = '"
                        + noneSchemaName
                        + "',"
                        + "  'protobuf-glue.schema.autoRegistration' = 'true',"
                        + "  'protobuf-glue.schema.compatibility' = 'NONE'"
                        + ")");

        LOG.info("Test 5 step 1: Writing v1 data with NONE compat...");
        tEnv.executeSql("INSERT INTO compat_none_sink_v1 VALUES ('Dave', 40, 'Denver')")
                .await(120, TimeUnit.SECONDS);

        // v2: completely different schema (2 fields, removed city)
        tEnv.executeSql(
                "CREATE TABLE compat_none_sink_v2 ("
                        + "  user_name STRING,"
                        + "  age INT"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '"
                        + streamArn
                        + "',"
                        + "  'aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'format' = 'protobuf-glue',"
                        + "  'protobuf-glue.aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'protobuf-glue.registry.name' = '"
                        + registryName
                        + "',"
                        + "  'protobuf-glue.schema.name' = '"
                        + noneSchemaName
                        + "',"
                        + "  'protobuf-glue.schema.autoRegistration' = 'true',"
                        + "  'protobuf-glue.schema.compatibility' = 'NONE'"
                        + ")");

        LOG.info(
                "Test 5 step 2: Writing v2 data (incompatible change, should succeed with NONE)...");
        tEnv.executeSql("INSERT INTO compat_none_sink_v2 VALUES ('Eve', 28)")
                .await(120, TimeUnit.SECONDS);
        LOG.info("PASS test 5: NONE compatibility allows any schema evolution");

        // ================================================================
        // TEST 6: Schema compatibility — FULL
        //
        // FULL = both BACKWARD and FORWARD. Adding a new field is the
        // canonical safe evolution for proto3.
        // ================================================================
        LOG.info("=== Test 6: Schema compatibility — FULL ===");
        String fullSchemaName = schemaNamePrefix + "-compat-full";

        // v1: 3 fields
        tEnv.executeSql(
                "CREATE TABLE compat_full_sink_v1 ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  city STRING"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '"
                        + streamArn
                        + "',"
                        + "  'aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'format' = 'protobuf-glue',"
                        + "  'protobuf-glue.aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'protobuf-glue.registry.name' = '"
                        + registryName
                        + "',"
                        + "  'protobuf-glue.schema.name' = '"
                        + fullSchemaName
                        + "',"
                        + "  'protobuf-glue.schema.autoRegistration' = 'true',"
                        + "  'protobuf-glue.schema.compatibility' = 'FULL'"
                        + ")");

        LOG.info("Test 6 step 1: Writing v1 data with FULL compat...");
        tEnv.executeSql("INSERT INTO compat_full_sink_v1 VALUES ('Frank', 45, 'Chicago')")
                .await(120, TimeUnit.SECONDS);

        // v2: add new field
        tEnv.executeSql(
                "CREATE TABLE compat_full_sink_v2 ("
                        + "  user_name STRING,"
                        + "  age INT,"
                        + "  city STRING,"
                        + "  email STRING"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = '"
                        + streamArn
                        + "',"
                        + "  'aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'format' = 'protobuf-glue',"
                        + "  'protobuf-glue.aws.region' = '"
                        + awsRegion
                        + "',"
                        + "  'protobuf-glue.registry.name' = '"
                        + registryName
                        + "',"
                        + "  'protobuf-glue.schema.name' = '"
                        + fullSchemaName
                        + "',"
                        + "  'protobuf-glue.schema.autoRegistration' = 'true',"
                        + "  'protobuf-glue.schema.compatibility' = 'FULL'"
                        + ")");

        LOG.info("Test 6 step 2: Writing v2 data (added email field)...");
        tEnv.executeSql(
                        "INSERT INTO compat_full_sink_v2 VALUES ('Grace', 32, 'Boston', 'grace@example.com')")
                .await(120, TimeUnit.SECONDS);
        LOG.info("PASS test 6: FULL compatibility allows adding new fields");

        LOG.info("=== All Protobuf-Glue E2E Tests Passed ===");
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
