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

package org.apache.samza.container.grouper.task

import org.apache.samza.job.model.TaskModel
import org.apache.samza.job.model.ContainerModel
import scala.collection.JavaConversions._
import java.util

/**
 * Group the SSP taskNames by dividing the number of taskNames into the number
 * of containers (n) and assigning n taskNames to each container as returned by
 * iterating over the keys in the map of taskNames (whatever that ordering
 * happens to be). No consideration is given towards locality, even distribution
 * of aggregate SSPs within a container, even distribution of the number of
 * taskNames between containers, etc.
 */
class GroupByContainerCount(numContainers: Int) extends TaskNameGrouper {
  require(numContainers > 0, "Must have at least one container")

  override def group(tasks: util.Set[TaskModel]): util.Set[ContainerModel] = {
    require(tasks.size > 0, "No tasks found. Likely due to no input partitions. Can't run a job with no tasks.")
    require(tasks.size >= numContainers, "Your container count (%s) is larger than your task count (%s). Can't have containers with nothing to do, so aborting." format (numContainers, tasks.size))
    setAsJavaSet(tasks
      .toList
      // Sort tasks by taskName.
      .sortWith { case (task1, task2) => task1.compareTo(task2) < 0 }
      // Assign every task an ID.
      .zip(0 until tasks.size)
      // Map every task to a container using its task ID.
      .groupBy(_._2 % numContainers)
      // Take just TaskModel and remove task IDs.
      .mapValues(_.map { case (task, taskId) => (task.getTaskName, task) }.toMap)
      .map { case (containerId, taskModels) => new ContainerModel(containerId, taskModels) }
      .toSet)
  }
}

