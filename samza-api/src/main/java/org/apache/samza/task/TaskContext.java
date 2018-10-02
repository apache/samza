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

package org.apache.samza.task;

import java.util.Set;

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.scheduler.ScheduledCallback;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.table.Table;


/**
 * A TaskContext provides resources about the {@link org.apache.samza.task.StreamTask}, particularly during
 * initialization in an {@link org.apache.samza.task.InitableTask}.
 * TODO this will be replaced by {@link org.apache.samza.context.TaskContext} in the near future by SAMZA-1714
 */
public interface TaskContext {
  MetricsRegistry getMetricsRegistry();

  Set<SystemStreamPartition> getSystemStreamPartitions();

  Object getStore(String name);

  Table getTable(String tableId);

  TaskName getTaskName();

  SamzaContainerContext getSamzaContainerContext();

  /**
   * Set the starting offset for the given {@link org.apache.samza.system.SystemStreamPartition}. Offsets
   * can only be set for a {@link org.apache.samza.system.SystemStreamPartition} assigned to this task
   * (as returned by {@link #getSystemStreamPartitions()}); trying to set the offset for any other partition
   * will have no effect.
   *
   * NOTE: this feature is experimental, and the API may change in a future release.
   *
   * @param ssp {@link org.apache.samza.system.SystemStreamPartition} whose offset should be set
   * @param offset to set for the given {@link org.apache.samza.system.SystemStreamPartition}
   *
   */
  void setStartingOffset(SystemStreamPartition ssp, String offset);

  /**
   * Sets the user-defined context.
   *
   * @param context the user-defined context to set
   */
  default void setUserContext(Object context) { }

  /**
   * Gets the user-defined context.
   *
   * @return the user-defined context if set, else null
   */
  default Object getUserContext() {
    return null;
  }

  /**
   * Schedule the {@code callback} for the provided {@code key} to be invoked at epoch-time {@code timestamp}.
   * The callback will be invoked exclusively with any other operations for this task,
   * e.g. processing, windowing and commit.
   * @param key key for the callback
   * @param timestamp epoch time when the callback will be fired, in milliseconds
   * @param callback callback to call when the {@code timestamp} is reached
   * @param <K> type of the key
   */
  <K> void scheduleCallback(K key, long timestamp, ScheduledCallback<K> callback);

  /**
   * Delete the scheduled {@code callback} for the {@code key}.
   * Deletion only happens if the callback hasn't been fired. Otherwise it will not interrupt.
   * @param key callback key
   * @param <K> type of the key
   */
  <K> void deleteScheduledCallback(K key);
}
