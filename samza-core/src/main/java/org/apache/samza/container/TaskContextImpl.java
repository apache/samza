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

package org.apache.samza.container;

import com.google.common.collect.ImmutableSet;
import java.util.function.Function;
import org.apache.samza.checkpoint.OffsetManager;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableManager;
import org.apache.samza.task.EpochTimeScheduler;
import org.apache.samza.task.TaskContext;
import org.apache.samza.scheduler.ScheduledCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

public class TaskContextImpl implements TaskContext {
  private static final Logger LOG = LoggerFactory.getLogger(TaskContextImpl.class);

  private final TaskName taskName;
  private final TaskInstanceMetrics metrics;
  private final SamzaContainerContext containerContext;
  private final Set<SystemStreamPartition> systemStreamPartitions;
  private final OffsetManager offsetManager;
  private final Function<String, KeyValueStore> kvStoreSupplier;
  private final TableManager tableManager;
  private final JobModel jobModel;
  private final StreamMetadataCache streamMetadataCache;
  private final Map<String, Object> objectRegistry = new HashMap<>();
  private final EpochTimeScheduler timerScheduler;

  private Object userContext = null;

  public TaskContextImpl(TaskName taskName,
                         TaskInstanceMetrics metrics,
                         SamzaContainerContext containerContext,
                         Set<SystemStreamPartition> systemStreamPartitions,
                         OffsetManager offsetManager,
                         Function<String, KeyValueStore> kvStoreSupplier,
                         TableManager tableManager,
                         JobModel jobModel,
                         StreamMetadataCache streamMetadataCache,
                         ScheduledExecutorService timerExecutor) {
    this.taskName = taskName;
    this.metrics = metrics;
    this.containerContext = containerContext;
    this.systemStreamPartitions = ImmutableSet.copyOf(systemStreamPartitions);
    this.offsetManager = offsetManager;
    this.kvStoreSupplier = kvStoreSupplier;
    this.tableManager = tableManager;
    this.jobModel = jobModel;
    this.streamMetadataCache = streamMetadataCache;
    this.timerScheduler = EpochTimeScheduler.create(timerExecutor);
  }

  @Override
  public ReadableMetricsRegistry getMetricsRegistry() {
    return metrics.registry();
  }

  @Override
  public Set<SystemStreamPartition> getSystemStreamPartitions() {
    return systemStreamPartitions;
  }

  @Override
  public KeyValueStore getStore(String storeName) {
    KeyValueStore store = kvStoreSupplier.apply(storeName);
    if (store == null) {
      LOG.warn("No store found for name: {}", storeName);
    }
    return store;
  }

  @Override
  public Table getTable(String tableId) {
    if (tableManager != null) {
      return tableManager.getTable(tableId);
    } else {
      LOG.warn("No table manager found");
      return null;
    }
  }

  @Override
  public TaskName getTaskName() {
    return taskName;
  }

  @Override
  public SamzaContainerContext getSamzaContainerContext() {
    return containerContext;
  }

  @Override
  public void setStartingOffset(SystemStreamPartition ssp, String offset) {
    offsetManager.setStartingOffset(taskName, ssp, offset);
  }

  @Override
  public void setUserContext(Object context) {
    userContext = context;
  }

  @Override
  public Object getUserContext() {
    return userContext;
  }

  @Override
  public <K> void scheduleCallback(K key, long timestamp, ScheduledCallback<K> callback) {
    timerScheduler.setTimer(key, timestamp, callback);
  }

  @Override
  public <K> void deleteScheduledCallback(K key) {
    timerScheduler.deleteTimer(key);
  }

  public void registerObject(String name, Object value) {
    objectRegistry.put(name, value);
  }

  public Object fetchObject(String name) {
    return objectRegistry.get(name);
  }

  public JobModel getJobModel() {
    return jobModel;
  }

  public StreamMetadataCache getStreamMetadataCache() {
    return streamMetadataCache;
  }

  public EpochTimeScheduler getTimerScheduler() {
    return timerScheduler;
  }
}
