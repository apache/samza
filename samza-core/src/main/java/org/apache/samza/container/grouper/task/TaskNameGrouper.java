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
package org.apache.samza.container.grouper.task;

import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;

import java.util.Set;

/**
 * <p>
 * After the input SystemStreamPartitions have been mapped to their tasks by an
 * implementation of
 * {@link org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper}
 * , we can then map those groupings into the
 * SamzaContainers on which they will run.
 * This class takes a set of TaskModels and groups them together into
 * ContainerModels. All tasks within a single ContainerModel will be executed in
 * a single SamzaContainer.
 * </p>
 *
 * <p>
 * A simple implementation could assign each TaskModel to a separate container.
 * More advanced implementations could examine the TaskModel to group them by
 * data locality, anti-affinity, even distribution of expected bandwidth
 * consumption, etc.
 * </p>
 */
public interface TaskNameGrouper {
  /**
   * Group tasks into the containers they will share.
   *
   * @param tasks Set of tasks to group into containers.
   * @return Set of containers, which contain the tasks that were passed in.
   */
  Set<ContainerModel> group(Set<TaskModel> tasks);
}
