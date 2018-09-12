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

import java.util.Set;
import java.util.function.Function;
import org.apache.samza.checkpoint.OffsetManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.scheduling.Scheduler;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableManager;


public class TaskContextImpl implements TaskContext {
  private final TaskName taskName;
  private final Set<SystemStreamPartition> systemStreamPartitions;
  private final MetricsRegistry taskMetricsRegistry;
  private final Function<String, KeyValueStore> keyValueStoreProvider;
  private final TableManager tableManager;
  private final Scheduler scheduler;
  private final OffsetManager offsetManager;

  public TaskContextImpl(TaskName taskName, Set<SystemStreamPartition> systemStreamPartitions,
      MetricsRegistry taskMetricsRegistry, Function<String, KeyValueStore> keyValueStoreProvider,
      TableManager tableManager, Scheduler scheduler, OffsetManager offsetManager) {
    this.taskName = taskName;
    this.systemStreamPartitions = systemStreamPartitions;
    this.taskMetricsRegistry = taskMetricsRegistry;
    this.keyValueStoreProvider = keyValueStoreProvider;
    this.tableManager = tableManager;
    this.scheduler = scheduler;
    this.offsetManager = offsetManager;
  }

  @Override
  public TaskName getTaskName() {
    return this.taskName;
  }

  @Override
  public Set<SystemStreamPartition> getSystemStreamPartitions() {
    return this.systemStreamPartitions;
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
  public Table getTable(String tableId) {
    return this.tableManager.getTable(tableId);
  }

  @Override
  public Scheduler getScheduler() {
    return this.scheduler;
  }

  @Override
  public void setStartingOffset(SystemStreamPartition systemStreamPartition, String offset) {
    this.offsetManager.setStartingOffset(this.taskName, systemStreamPartition, offset);
  }
}
