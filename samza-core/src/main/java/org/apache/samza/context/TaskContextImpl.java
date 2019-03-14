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
package org.apache.samza.context;

import org.apache.samza.checkpoint.OffsetManager;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.scheduler.CallbackScheduler;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.TableManager;

import java.util.function.Function;


public class TaskContextImpl implements TaskContext {
  private final TaskModel taskModel;
  private final MetricsRegistry taskMetricsRegistry;
  private final Function<String, KeyValueStore> keyValueStoreProvider;
  private final TableManager tableManager;
  private final CallbackScheduler callbackScheduler;
  private final OffsetManager offsetManager;
  private final JobModel jobModel;
  private final StreamMetadataCache streamMetadataCache;

  public TaskContextImpl(TaskModel taskModel,
      MetricsRegistry taskMetricsRegistry,
      Function<String, KeyValueStore> keyValueStoreProvider,
      TableManager tableManager,
      CallbackScheduler callbackScheduler,
      OffsetManager offsetManager,
      JobModel jobModel,
      StreamMetadataCache streamMetadataCache) {
    this.taskModel = taskModel;
    this.taskMetricsRegistry = taskMetricsRegistry;
    this.keyValueStoreProvider = keyValueStoreProvider;
    this.tableManager = tableManager;
    this.callbackScheduler = callbackScheduler;
    this.offsetManager = offsetManager;
    this.jobModel = jobModel;
    this.streamMetadataCache = streamMetadataCache;
  }

  @Override
  public TaskModel getTaskModel() {
    return this.taskModel;
  }

  @Override
  public MetricsRegistry getTaskMetricsRegistry() {
    return this.taskMetricsRegistry;
  }

  @Override
  public KeyValueStore getStore(String storeName) {
    KeyValueStore store = this.keyValueStoreProvider.apply(storeName);
    if (store == null) {
      throw new IllegalArgumentException(String.format("No store found for storeName: %s", storeName));
    }
    return store;
  }

  @Override
  public <K, V> ReadWriteTable<K, V> getTable(String tableId) {
    return this.tableManager.getTable(tableId);
  }

  @Override
  public CallbackScheduler getCallbackScheduler() {
    return this.callbackScheduler;
  }

  @Override
  public void setStartingOffset(SystemStreamPartition systemStreamPartition, String offset) {
    this.offsetManager.setStartingOffset(this.taskModel.getTaskName(), systemStreamPartition, offset);
  }

  public JobModel getJobModel() {
    return this.jobModel;
  }

  public StreamMetadataCache getStreamMetadataCache() {
    return this.streamMetadataCache;
  }
}
