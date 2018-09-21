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
import org.apache.samza.table.Table;


/**
 * Contains information at task granularity, provided by the Samza framework, to be used to instantiate an application
 * at runtime.
 * <p>
 * Note that application-defined task-level context is accessible through {@link ApplicationTaskContext}.
 */
public interface TaskContext {
  /**
   * Returns the {@link TaskModel} associated with this task. This contains information like the task name and
   * associated {@link SystemStreamPartition}s.
   * @return {@link TaskModel} associated with this task
   */
  TaskModel getTaskModel();

  /**
   * Returns the {@link MetricsRegistry} for this task. Metrics built using this registry will be associated with the
   * task.
   * @return {@link MetricsRegistry} for this task
   */
  MetricsRegistry getTaskMetricsRegistry();

  /**
   * Returns the {@link KeyValueStore} corresponding to the {@code storeName}. In application code, it is recommended to
   * cast the resulting stores to {@link KeyValueStore}s with the correct concrete type parameters.
   * @param storeName name of the {@link KeyValueStore} to get
   * @return {@link KeyValueStore} corresponding to the {@code storeName}
   * @throws IllegalArgumentException if there is no store associated with {@code storeName}
   */
  KeyValueStore<?, ?> getStore(String storeName);

  /**
   * Returns the {@link Table} corresponding to the {@code tableId}. In application code, it is recommended to cast this
   * to the resulting tables to {@link Table}s with the correct concrete type parameters.
   * @param tableId id of the {@link Table} to get
   * @return {@link Table} corresponding to the {@code tableId}
   * @throws IllegalArgumentException if there is no table associated with {@code tableId}
   */
  Table<?> getTable(String tableId);

  /**
   * Returns a task-level {@link CallbackScheduler} which can be used to delay execution of some logic.
   * @return {@link CallbackScheduler} for this task
   */
  CallbackScheduler getCallbackScheduler();

  /**
   * Set the starting offset for the given {@link SystemStreamPartition}. Offsets can only be set for a
   * {@link SystemStreamPartition} assigned to this task. The {@link SystemStreamPartition}s assigned to this task can
   * be accessed through {@link TaskModel#getSystemStreamPartitions()} for the {@link TaskModel} obtained by calling
   * {@link #getTaskModel()}. Trying to set the offset for any other partition will have no effect.
   *
   * NOTE: this feature is experimental, and the API may change in a future release.
   *
   * @param systemStreamPartition {@link org.apache.samza.system.SystemStreamPartition} whose offset should be set
   * @param offset to set for the given {@link org.apache.samza.system.SystemStreamPartition}
   */
  @InterfaceStability.Evolving
  void setStartingOffset(SystemStreamPartition systemStreamPartition, String offset);
}