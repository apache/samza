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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.scheduler.CallbackScheduler;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.Table;


/**
 * The framework-provided context for the current task.
 * <p>
 * Use {@link ApplicationTaskContext} for the application-defined context for the current task.
 */
public interface TaskContext {

  /**
   * Gets the {@link TaskModel} for this task, which contains this task's name and its {@link SystemStreamPartition}s.
   *
   * @return the {@link TaskModel} for this task
   */
  TaskModel getTaskModel();

  /**
   * Gets the {@link MetricsRegistry} for this task, which can be used to register metrics that are reported per task.
   *
   * @return the {@link MetricsRegistry} for this task
   */
  MetricsRegistry getTaskMetricsRegistry();

  /**
   * Gets the {@link KeyValueStore} associated with {@code storeName} for this task.
   * <p>
   * The returned store should be cast with the concrete type parameters based on the configured store serdes.
   * E.g., if using string key and integer value serde, it should be cast to a {@code KeyValueStore<String, Integer>}.
   *
   * @param storeName name of the {@link KeyValueStore} to get for this task
   * @return the {@link KeyValueStore} associated with {@code storeName} for this task
   * @throws IllegalArgumentException if there is no store associated with {@code storeName}
   */
  KeyValueStore<?, ?> getStore(String storeName);

  /**
   * Gets the {@link Table} corresponding to the {@code tableId} for this task.
   *
   * The returned table should be cast with the concrete type parameters based on the configured table serdes, and
   * whether it is {@link ReadWriteTable} or {@link ReadableTable}. E.g., if using string key and integer value
   * serde for a writable table, it should be cast to a {@code ReadWriteTable<String, Integer>}.
   *
   * @param tableId id of the {@link Table} to get
   * @return the {@link Table} associated with {@code tableId} for this task
   * @throws IllegalArgumentException if there is no table associated with {@code tableId}
   */
  Table<?> getTable(String tableId);

  /**
   * Gets the {@link CallbackScheduler} for this task, which can be used to schedule a callback to be executed
   * at a future time.
   *
   * @return the {@link CallbackScheduler} for this task
   */
  CallbackScheduler getCallbackScheduler();

  /**
   * Sets the starting offset for the given {@link SystemStreamPartition}.
   * <p> Offsets can only be set for a {@link SystemStreamPartition} assigned to this task.
   * The {@link SystemStreamPartition}s assigned to this task can be accessed through
   * {@link TaskModel#getSystemStreamPartitions()} for the {@link TaskModel} obtained by calling
   * {@link #getTaskModel()}. Trying to set the offset for any other partition will have no effect.
   *
   * NOTE: this feature is experimental, and the API may change in a future release.
   *
   * @param systemStreamPartition {@link SystemStreamPartition} whose offset should be set
   * @param offset to set for the given {@link SystemStreamPartition}
   */
  @InterfaceStability.Evolving
  void setStartingOffset(SystemStreamPartition systemStreamPartition, String offset);
}