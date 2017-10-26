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

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableQuery;
import org.apache.samza.AzureClient;
import org.apache.samza.AzureException;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.AzureConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AzureCheckpointManager implements CheckpointManager {
  private static final Logger LOG = LoggerFactory.getLogger(AzureCheckpointManager.class.getName());
  private Set<TaskName> taskNames = new HashSet<>();
  private static final String PARTITION_KEY = "PartitionKey";

  public static final String CHECKPOINT_MANAGER_TABLE_NAME = "Task_Checkpoints";
  // Define the connection-string with your values.
  private String storageConnectionString =
          "DefaultEndpointsProtocol=http;" +
                  "AccountName=your_storage_account;" +
                  "AccountKey=your_storage_account_key";
  private AzureClient azureClient;
  private CloudTable cloudTable;

  AzureCheckpointManager(AzureConfig azureConfig) {
    storageConnectionString = azureConfig.getAzureConnect();
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
    } catch (StorageException e) {
      LOG.error("Azure Storage failed when creating a table", e);
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

    checkpoint.getOffsets().forEach((ssp, offset) -> {
        TaskCheckpointEntity taskCheckpoint = new TaskCheckpointEntity(taskName.toString(), ssp.getSystem());
        taskCheckpoint.setStreamName(ssp.getStream());
        taskCheckpoint.setPartitionId(ssp.getPartition().getPartitionId());
        taskCheckpoint.setOffset(offset);
        batchOperation.insertOrReplace(taskCheckpoint);
      });

    try {
      cloudTable.execute(batchOperation);
    } catch (StorageException e) {
      LOG.error("Connection string {} specifies an invalid key.", storageConnectionString);
      throw new AzureException(e);
    }
  }

  @Override
  public Checkpoint readLastCheckpoint(TaskName taskName) {
    if (!taskNames.contains(taskName)) {
      throw new SamzaException("writing checkpoint of unregistered task");
    }
    String partitionQueryKey = taskName.toString();
    String partitionFilter = TableQuery.generateFilterCondition(
            PARTITION_KEY,
            TableQuery.QueryComparisons.EQUAL,
            partitionQueryKey);

    TableQuery<TaskCheckpointEntity> partitionQuery = TableQuery.from(TaskCheckpointEntity.class)
            .where(partitionFilter);
    Map<SystemStreamPartition, String> offsets = new HashMap<>();
    for (TaskCheckpointEntity taskCheckpointEntity : cloudTable.execute(partitionQuery)) {
      String systemName = taskCheckpointEntity.getRowKey();
      String streamName = taskCheckpointEntity.getStreamName();
      int partitionId = taskCheckpointEntity.getPartitionId();
      offsets.put(new SystemStreamPartition(systemName, streamName, new Partition(partitionId)),
              taskCheckpointEntity.getOffset());
    }
    return new Checkpoint(offsets);
  }

  @Override
  public void stop() {
    try {
      cloudTable.deleteIfExists();
    } catch (StorageException e) {
      LOG.error("Azure Storage failed when creating a table", e);
    }
    taskNames.clear();
  }
}
