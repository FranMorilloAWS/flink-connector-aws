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

package org.apache.flink.table.catalog.glue.operator;

import org.apache.flink.table.catalog.exceptions.CatalogException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;

import java.util.ArrayList;
import java.util.List;

/**
 * Handles partition operations for the Glue catalog. Provides functionality for listing,
 * retrieving, creating, updating and deleting partitions in AWS Glue, with pagination handled
 * internally.
 */
public class GluePartitionOperator extends GlueOperator {

    private static final Logger LOG = LoggerFactory.getLogger(GluePartitionOperator.class);

    /**
     * Constructor for GluePartitionOperator.
     *
     * @param glueClient The Glue client to use for partition operations.
     * @param catalogName The name of the catalog.
     */
    public GluePartitionOperator(GlueClient glueClient, String catalogName) {
        super(glueClient, catalogName);
    }

    /**
     * Lists all partitions of a table, following pagination.
     *
     * @param databaseName The name of the database containing the table.
     * @param tableName The name of the table.
     * @return All partitions of the table.
     * @throws CatalogException if an error occurs while listing partitions.
     */
    public List<Partition> listPartitions(String databaseName, String tableName) {
        try {
            List<Partition> partitions = new ArrayList<>();
            String nextToken = null;
            do {
                GetPartitionsRequest.Builder requestBuilder =
                        GetPartitionsRequest.builder()
                                .databaseName(databaseName)
                                .tableName(tableName);
                if (nextToken != null) {
                    requestBuilder.nextToken(nextToken);
                }
                GetPartitionsResponse response = glueClient.getPartitions(requestBuilder.build());
                if (response.partitions() != null) {
                    partitions.addAll(response.partitions());
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
            return partitions;
        } catch (GlueException e) {
            throw new CatalogException("Error listing partitions: " + e.getMessage(), e);
        }
    }

    /**
     * Gets a single partition by its ordered partition values.
     *
     * @param databaseName The name of the database containing the table.
     * @param tableName The name of the table.
     * @param partitionValues The partition values, ordered by the table's partition keys.
     * @return The partition, or {@code null} if it does not exist.
     * @throws CatalogException if an error occurs while getting the partition.
     */
    public Partition getPartition(
            String databaseName, String tableName, List<String> partitionValues) {
        try {
            GetPartitionRequest request =
                    GetPartitionRequest.builder()
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .partitionValues(partitionValues)
                            .build();
            return glueClient.getPartition(request).partition();
        } catch (EntityNotFoundException e) {
            return null;
        } catch (GlueException e) {
            throw new CatalogException("Error getting partition: " + e.getMessage(), e);
        }
    }

    /**
     * Creates a partition. Glue's {@code AlreadyExistsException} is propagated to the caller so
     * catalog-level semantics (ignore-if-exists) can be applied there.
     *
     * @param databaseName The name of the database containing the table.
     * @param tableName The name of the table.
     * @param partitionInput The Glue partition input.
     * @throws software.amazon.awssdk.services.glue.model.AlreadyExistsException if the partition
     *     already exists.
     * @throws CatalogException if any other error occurs while creating the partition.
     */
    public void createPartition(
            String databaseName, String tableName, PartitionInput partitionInput) {
        try {
            CreatePartitionRequest request =
                    CreatePartitionRequest.builder()
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .partitionInput(partitionInput)
                            .build();
            glueClient.createPartition(request);
            LOG.debug(
                    "Created partition {} in {}.{}",
                    partitionInput.values(),
                    databaseName,
                    tableName);
        } catch (software.amazon.awssdk.services.glue.model.AlreadyExistsException e) {
            throw e;
        } catch (GlueException e) {
            throw new CatalogException("Error creating partition: " + e.getMessage(), e);
        }
    }

    /**
     * Updates an existing partition. Glue's {@code EntityNotFoundException} is propagated to the
     * caller so catalog-level semantics (ignore-if-not-exists) can be applied there.
     *
     * @param databaseName The name of the database containing the table.
     * @param tableName The name of the table.
     * @param partitionValues The current partition values identifying the partition.
     * @param partitionInput The new Glue partition input.
     * @throws EntityNotFoundException if the partition does not exist.
     * @throws CatalogException if any other error occurs while updating the partition.
     */
    public void updatePartition(
            String databaseName,
            String tableName,
            List<String> partitionValues,
            PartitionInput partitionInput) {
        try {
            UpdatePartitionRequest request =
                    UpdatePartitionRequest.builder()
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .partitionValueList(partitionValues)
                            .partitionInput(partitionInput)
                            .build();
            glueClient.updatePartition(request);
            LOG.debug("Updated partition {} in {}.{}", partitionValues, databaseName, tableName);
        } catch (EntityNotFoundException e) {
            throw e;
        } catch (GlueException e) {
            throw new CatalogException("Error updating partition: " + e.getMessage(), e);
        }
    }

    /**
     * Deletes a partition. Glue's {@code EntityNotFoundException} is propagated to the caller so
     * catalog-level semantics (ignore-if-not-exists) can be applied there.
     *
     * @param databaseName The name of the database containing the table.
     * @param tableName The name of the table.
     * @param partitionValues The partition values identifying the partition.
     * @throws EntityNotFoundException if the partition does not exist.
     * @throws CatalogException if any other error occurs while deleting the partition.
     */
    public void dropPartition(String databaseName, String tableName, List<String> partitionValues) {
        try {
            DeletePartitionRequest request =
                    DeletePartitionRequest.builder()
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .partitionValues(partitionValues)
                            .build();
            glueClient.deletePartition(request);
            LOG.debug("Dropped partition {} in {}.{}", partitionValues, databaseName, tableName);
        } catch (EntityNotFoundException e) {
            throw e;
        } catch (GlueException e) {
            throw new CatalogException("Error dropping partition: " + e.getMessage(), e);
        }
    }
}
