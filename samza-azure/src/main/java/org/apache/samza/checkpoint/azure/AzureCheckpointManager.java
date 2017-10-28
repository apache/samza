/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.samza.checkpoint.azure;

import com.google.common.collect.ImmutableMap;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.*;
import org.apache.samza.AzureClient;
import org.apache.samza.AzureException;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.AzureConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Azure checkpoint manager is used to store checkpoints in a Azure Table.
 * All the task checkpoints are added to the a single table named "SamzaTaskCheckpoints".
 * The table entities take the following form:
 *
 * +-----------------+---------------------+-------------------+
 * |                 |     Serialized      |                   |
 * |   TaskName      |     JSON SSP        |     Offset        |
 * |                 |                     |                   |
 * +-----------------+---------------------+-------------------+
 *
 *  Each entity have a partitionKey set as the TaskName and the rowKey set as the SSP.
 */
public class AzureCheckpointManager implements CheckpointManager {
  private static final Logger LOG = LoggerFactory.getLogger(AzureCheckpointManager.class.getName());
  private static final String PARTITION_KEY = "PartitionKey";

  public static final int MAX_WRITE_BATCH_SIZE = 100;
  public static final String CHECKPOINT_MANAGER_TABLE_NAME = "SamzaTaskCheckpoints";
  public static final String SYSTEM_PROP_NAME = "system";
  public static final String STREAM_PROP_NAME = "stream";
  public static final String PARTITION_PROP_NAME = "partition";

  private final String storageConnectionString;
  private final AzureClient azureClient;
  private CloudTable cloudTable;

  private final Set<TaskName> taskNames = new HashSet<>();
  private final JsonSerdeV2<Map<String, String>> jsonSerde = new JsonSerdeV2<>();

  AzureCheckpointManager(AzureConfig azureConfig) {
    storageConnectionString = azureConfig.getAzureConnectionString();
    azureClient = new AzureClient(storageConnectionString);
  }

  @Override
  public void start() {
    try {
      // Create the table if it doesn't exist.
      cloudTable = azureClient.getTableClient().getTableReference(CHECKPOINT_MANAGER_TABLE_NAME);
      cloudTable.createIfNotExists();

    } catch (URISyntaxException e) {
      LOG.error("Connection string {} specifies an invalid URI while creating checkpoint table.",
              storageConnectionString);
      throw new AzureException(e);

    } catch (StorageException e) {
      LOG.error("Azure Storage failed when creating checkpoint table", e);
      throw new AzureException(e);
    }
  }

  @Override
  public void register(TaskName taskName) {
    taskNames.add(taskName);
  }

  @Override
  public void writeCheckpoint(TaskName taskName, Checkpoint checkpoint) {
    if (!taskNames.contains(taskName)) {
      throw new SamzaException("writing checkpoint of unregistered task");
    }

    TableBatchOperation batchOperation = new TableBatchOperation();

    Iterator<Map.Entry<SystemStreamPartition, String>> iterator = checkpoint.getOffsets().entrySet().iterator();
    while(iterator.hasNext()) {
      Map.Entry<SystemStreamPartition, String> entry = iterator.next();
      SystemStreamPartition ssp = entry.getKey();
      String offset = entry.getValue();

      // Create table entity
      TaskCheckpointEntity taskCheckpoint = new TaskCheckpointEntity(taskName.toString(),
              serializeSystemStreamPartition(ssp), offset);

      // Add to batch operation
      batchOperation.insertOrReplace(taskCheckpoint);

      // Execute when batch reaches capacity or this is the last item
      if (batchOperation.size() >= MAX_WRITE_BATCH_SIZE || !iterator.hasNext()) {
        try {
          cloudTable.execute(batchOperation);
        } catch (StorageException e) {
          LOG.error("Executing batch failed for task: {}", taskName);
          throw new AzureException(e);
        }
        batchOperation.clear();
      }
    }
  }

  private String serializeSystemStreamPartition(SystemStreamPartition ssp) {
    // Create the Json string for SystemStreamPartition
    Map<String, String> sspMap = new HashMap<>();

    sspMap.put(SYSTEM_PROP_NAME, ssp.getSystem());
    sspMap.put(STREAM_PROP_NAME, ssp.getStream());
    sspMap.put(PARTITION_PROP_NAME, String.valueOf(ssp.getPartition().getPartitionId()));

    return new String(jsonSerde.toBytes(sspMap));
  }

  private SystemStreamPartition deserializeSystemStreamPartition(String serializedSSP) {
    Map<String, String> sspPropertiesMap = jsonSerde.fromBytes(serializedSSP.getBytes());

    String systemName = sspPropertiesMap.get(SYSTEM_PROP_NAME);
    String streamName = sspPropertiesMap.get(STREAM_PROP_NAME);
    Partition partition = new Partition(Integer.parseInt(sspPropertiesMap.get("partition")));

    return new SystemStreamPartition(systemName, streamName, partition);
  }

  @Override
  public Checkpoint readLastCheckpoint(TaskName taskName) {
    if (!taskNames.contains(taskName)) {
      throw new SamzaException("reading checkpoint of unregistered/unwritten task");
    }

    // Create the query for taskName
    String partitionQueryKey = taskName.toString();
    String partitionFilter = TableQuery.generateFilterCondition(
            PARTITION_KEY,
            TableQuery.QueryComparisons.EQUAL,
            partitionQueryKey);
    TableQuery<TaskCheckpointEntity> query = TableQuery.from(TaskCheckpointEntity.class).where(partitionFilter);

    ImmutableMap.Builder<SystemStreamPartition, String> builder = ImmutableMap.builder();
    try {
      for (TaskCheckpointEntity taskCheckpointEntity : cloudTable.execute(query)) {
        // Recreate the SSP offset
        String serializedSSP = taskCheckpointEntity.getRowKey();
        builder.put(deserializeSystemStreamPartition(serializedSSP), taskCheckpointEntity.getOffset());
      }

    } catch (NoSuchElementException e) {
      LOG.warn("No checkpoints found found for registered taskName={}", taskName);
      // Return null if not entity elements are not found
      return null;
    }
    LOG.debug("Received checkpoint state for taskName=%s", taskName);
    return new Checkpoint(builder.build());
  }

  @Override
  public void stop() {
    // Nothing to do here
  }

  @Override
  public void clearCheckpoints() {
    LOG.debug("Clearing all checkpoints in Azure table");

    for (TaskName taskName : taskNames) {
      String partitionQueryKey = taskName.toString();

      // Generate table query
      String partitionFilter = TableQuery.generateFilterCondition(
              PARTITION_KEY,
              TableQuery.QueryComparisons.EQUAL,
              partitionQueryKey);
      TableQuery<TaskCheckpointEntity> partitionQuery = TableQuery.from(TaskCheckpointEntity.class)
              .where(partitionFilter);

      // All entities in a given batch must have the same partition key
      deleteEntities(cloudTable.execute(partitionQuery).iterator());
    }
  }

  private void deleteEntities(Iterator<TaskCheckpointEntity> entitiesToDelete) {
    TableBatchOperation batchOperation = new TableBatchOperation();

    while(entitiesToDelete.hasNext()) {
      TaskCheckpointEntity entity = entitiesToDelete.next();

      // Add to batch operation
      batchOperation.delete(entity);

      // Execute when batch reaches capacity or when this is the last item
      if (batchOperation.size() >= MAX_WRITE_BATCH_SIZE || !entitiesToDelete.hasNext()) {
        try {
          cloudTable.execute(batchOperation);
        } catch (StorageException e) {
          LOG.error("Executing batch failed for deleting checkpoints");
          throw new AzureException(e);
        }
        batchOperation.clear();
      }
    }
  }
}
