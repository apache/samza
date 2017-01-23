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

import java.util.Collections;
import java.util.Set;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;


/**
 * Extends {@link TaskNameGrouper} with the ability to balance/redistribute the
 * tasks from a persisted task to container mapping. The goal of balancing is typically
 * to minimize the changes to the ContainerModels across job runs. This balance method returns
 * an equivalent set of {@link ContainerModel} as the group method, but it derives
 * from the persisted mapping, rather than from scratch. Thus the balance method is
 * called in lieu of group when the mapping is available and minimal changes are desired.
 *
 * {@inheritDoc}
 */
public interface BalancingTaskNameGrouper extends TaskNameGrouper {

  /**
   * Rebalances the tasks using the provided {@link LocalityManager}. The goal is typically
   * to minimize changes to the ContainerModels, e.g. when the container count changes.
   * This helps maximize the consistency of task-container locality, which is useful for optimization.
   * Each time balance() is called, locality information is read, it is used to balance the tasks,
   * and then the new locality information is saved.
   *
   * If balancing cannot be applied, then {@link TaskNameGrouper#group(Set)} should be used to
   * retrieve an appropriate set of ContainerModels. i.e. this method is a complete replacement
   * for {@link TaskNameGrouper#group(Set)}
   *
   * Implementations should prefer to use the previous mapping rather than calling
   * {@link TaskNameGrouper#group(Set)} to enable external custom task assignments.
   *
   * @param tasks           the tasks to group.
   * @param localityManager provides a persisted task to container map to use as a baseline
   * @return                the grouped tasks in the form of ContainerModels
   */
  default Set<ContainerModel> balance(Set<TaskModel> tasks, LocalityManager localityManager) {
    return Collections.<ContainerModel>emptySet();
  }

  default Set<ContainerModel> balance(Set<Integer> containerIds, Set<TaskModel> tasks, LocalityManager localityManager) {
    return Collections.<ContainerModel>emptySet();
  }
}
