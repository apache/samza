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

package org.apache.samza.util;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.samza.AzureClient;
import org.apache.samza.AzureException;
import org.apache.samza.coordinator.data.ProcessorEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  Client side class that has a reference to Azure Table Storage.
 *  Enables the user to add or delete information from the table, make updates to the table and retrieve information from the table.
 *  Every row in a table is uniquely identified by a combination of the PARTIITON KEY and ROW KEY.
 *  PARTITION KEY = Group ID = Job Model Version (for this case).
 *  ROW KEY = Unique entity ID for a group = Processor ID (for this case).
 */
public class TableUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TableUtils.class);
  private static final String PARTITION_KEY = "PartitionKey";
  private static final long LIVENESS_DEBOUNCE_TIME_SEC = 30;
  private final String initialState;
  private final CloudTableClient tableClient;
  private final CloudTable table;

  public TableUtils(AzureClient client, String tableName, String initialState) {
    this.initialState = initialState;
    tableClient = client.getTableClient();
    try {
      table = tableClient.getTableReference(tableName);
      table.createIfNotExists();
    } catch (URISyntaxException e) {
      LOG.error("\nConnection string specifies an invalid URI.", e);
      throw new AzureException(e);
    } catch (StorageException e) {
      LOG.error("Azure storage exception.", e);
      throw new AzureException(e);
    }
  }

  /**
   * Add a row which denotes an active processor to the processor table.
   * @param jmVersion Job model version that the processor is operating on.
   * @param pid Unique processor ID.
   * @param isLeader Denotes whether the processor is a leader or not.
   * @throws AzureException If an Azure storage service error occurred.
   */
  public void addProcessorEntity(String jmVersion, String pid, boolean isLeader) {
    ProcessorEntity entity = new ProcessorEntity(jmVersion, pid);
    entity.setIsLeader(isLeader);
    entity.updateLiveness();
    TableOperation add = TableOperation.insert(entity);
    try {
      table.execute(add);
    } catch (StorageException e) {
      LOG.error("Azure storage exception while adding processor entity with job model version: " + jmVersion + "and pid: " + pid, e);
      throw new AzureException(e);
    }
  }

  /**
   * Retrieve a particular row in the processor table, given the partition key and the row key.
   * @param jmVersion Job model version of the processor row to be retrieved.
   * @param pid Unique processor ID of the processor row to be retrieved.
   * @return An instance of required processor entity. Null if does not exist.
   * @throws AzureException If an Azure storage service error occurred.
   */
  public ProcessorEntity getEntity(String jmVersion, String pid) {
    try {
      TableOperation retrieveEntity = TableOperation.retrieve(jmVersion, pid, ProcessorEntity.class);
      ProcessorEntity entity = table.execute(retrieveEntity).getResultAsType();
      return entity;
    } catch (StorageException e) {
      LOG.error("Azure storage exception while retrieving processor entity with job model version: " + jmVersion + "and pid: " + pid, e);
      throw new AzureException(e);
    }
  }

  /**
   * Updates the liveness value of a particular processor with a randomly generated integer, which in turn updates the last modified since timestamp of the row.
   * @param jmVersion Job model version of the processor row to be updated.
   * @param pid Unique processor ID of the processor row to be updated.
   */
  public void updateHeartbeat(String jmVersion, String pid) {
    try {
      Random rand = new Random();
      TableOperation retrieveEntity = TableOperation.retrieve(jmVersion, pid, ProcessorEntity.class);
      ProcessorEntity entity = table.execute(retrieveEntity).getResultAsType();
      entity.updateLiveness();
      TableOperation update = TableOperation.replace(entity);
      table.execute(update);
    } catch (StorageException e) {
      LOG.error("Azure storage exception while updating heartbeat for job model version: " + jmVersion + "and pid: " + pid, e);
    }
  }

  /**
   * Updates the isLeader value when the processor starts or stops being a leader.
   * @param jmVersion Job model version of the processor row to be updated.
   * @param pid Unique processor ID of the processor row to be updated.
   * @param isLeader Denotes whether the processor is a leader or not.
   * @throws AzureException If an Azure storage service error occurred.
   */
  public void updateIsLeader(String jmVersion, String pid, boolean isLeader) {
    try {
      TableOperation retrieveEntity = TableOperation.retrieve(jmVersion, pid, ProcessorEntity.class);
      ProcessorEntity entity = table.execute(retrieveEntity).getResultAsType();
      entity.setIsLeader(isLeader);
      TableOperation update = TableOperation.replace(entity);
      table.execute(update);
    } catch (StorageException e) {
      LOG.error("Azure storage exception while updating isLeader value for job model version: " + jmVersion + "and pid: " + pid, e);
      throw new AzureException(e);
    }
  }

  /**
   * Deletes a specified row in the processor table.
   * @param jmVersion Job model version of the processor row to be deleted.
   * @param pid Unique processor ID of the processor row to be deleted.
   * @throws AzureException If an Azure storage service error occurred.
   */
  public void deleteProcessorEntity(String jmVersion, String pid) {
    try {
      TableOperation retrieveEntity = TableOperation.retrieve(jmVersion, pid, ProcessorEntity.class);
      ProcessorEntity entity = table.execute(retrieveEntity).getResultAsType();
      TableOperation remove = TableOperation.delete(entity);
      table.execute(remove);
    } catch (StorageException e) {
      LOG.error("Azure storage exception while deleting processor entity with job model version: " + jmVersion + "and pid: " + pid, e);
      throw new AzureException(e);
    }
  }

  /**
   * Retrieve all rows in a table with the given partition key.
   * @param partitionKey Job model version of the processors to be retrieved.
   * @return Iterable list of processor entities.
   */
  public Iterable<ProcessorEntity> getEntitiesWithPartition(String partitionKey) {
    String partitionFilter = TableQuery.generateFilterCondition(PARTITION_KEY, TableQuery.QueryComparisons.EQUAL, partitionKey);
    TableQuery<ProcessorEntity> partitionQuery = TableQuery.from(ProcessorEntity.class).where(partitionFilter);
    return table.execute(partitionQuery);
  }

  /**
   * Gets the list of all active processors that are heartbeating to the processor table.
   * @param currentJMVersion Current job model version that the processors in the application are operating on.
   * @return List of ids of currently active processors in the application, retrieved from the processor table.
   */
  public Set<String> getActiveProcessorsList(AtomicReference<String> currentJMVersion) {
    Iterable<ProcessorEntity> tableList = getEntitiesWithPartition(currentJMVersion.get());
    Set<String> activeProcessorsList = new HashSet<>();
    for (ProcessorEntity entity: tableList) {
      if (System.currentTimeMillis() - entity.getTimestamp().getTime() <= (LIVENESS_DEBOUNCE_TIME_SEC * 1000)) {
        activeProcessorsList.add(entity.getRowKey());
      }
    }

    Iterable<ProcessorEntity> unassignedList = getEntitiesWithPartition(initialState);
    for (ProcessorEntity entity: unassignedList) {
      long temp = System.currentTimeMillis() - entity.getTimestamp().getTime();
      LOG.info("Time elapsed since last heartbeat: {}", temp);
      if (temp <= (LIVENESS_DEBOUNCE_TIME_SEC * 1000)) {
        activeProcessorsList.add(entity.getRowKey());
      }
    }
    LOG.info("Active processors list: {}", activeProcessorsList);
    return activeProcessorsList;
  }

  public CloudTable getTable() {
    return table;
  }

}