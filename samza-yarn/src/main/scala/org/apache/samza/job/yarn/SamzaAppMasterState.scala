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

package org.apache.samza.job.yarn
import org.apache.samza.util.Logging
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.ContainerId
import java.util
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.container.TaskName

/**
 * Samza's application master has state that is usually manipulated based on
 * responses from YARN's resource manager (via SamzaAppMasterTaskManager). This
 * class holds the current state of the application master.
 */
class SamzaAppMasterState(val taskId: Int, val containerId: ContainerId, val nodeHost: String, val nodePort: Int, val nodeHttpPort: Int) extends YarnAppMasterListener with Logging {
  // controlled by the AM
  var completedTasks = 0
  var neededContainers = 0
  var failedContainers = 0
  var releasedContainers = 0
  var taskCount = 0
  var unclaimedTasks = Set[Int]()
  var finishedTasks = Set[Int]()
  var runningTasks = Map[Int, YarnContainer]()
  var taskToTaskNames = Map[Int, util.Map[TaskName, util.Set[SystemStreamPartition]]]()
  var status = FinalApplicationStatus.UNDEFINED
  var jobHealthy = true

  // controlled by the service
  var trackingPort = 0
  var rpcPort = 0

  // controlled on startup
  var appAttemptId = containerId.getApplicationAttemptId
}
