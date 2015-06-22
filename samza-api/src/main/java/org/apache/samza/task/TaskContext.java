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

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemStreamPartition;

import java.util.Set;

/**
 * A TaskContext provides resources about the {@link org.apache.samza.task.StreamTask}, particularly during
 * initialization in an {@link org.apache.samza.task.InitableTask}.
 */
public interface TaskContext {
  MetricsRegistry getMetricsRegistry();

  Set<SystemStreamPartition> getSystemStreamPartitions();

  Object getStore(String name);

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
}
