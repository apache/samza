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

package org.apache.samza.task

import org.apache.samza.task.TaskCoordinator.RequestScope
import org.apache.samza.Partition

/**
 * An in-memory implementation of TaskCoordinator that is specific to a single TaskInstance.
 */
class ReadableCoordinator(val partition: Partition) extends TaskCoordinator {
  var commitRequest: Option[RequestScope] = None
  var shutdownRequest: Option[RequestScope] = None

  override def commit(scope: RequestScope) { commitRequest = Some(scope) }
  override def shutdown(scope: RequestScope) { shutdownRequest = Some(scope) }

  def requestedCommitTask = commitRequest.isDefined && commitRequest.get == RequestScope.CURRENT_TASK
  def requestedCommitAll  = commitRequest.isDefined && commitRequest.get == RequestScope.ALL_TASKS_IN_CONTAINER

  def requestedShutdownOnConsensus = shutdownRequest.isDefined && shutdownRequest.get == RequestScope.CURRENT_TASK
  def requestedShutdownNow         = shutdownRequest.isDefined && shutdownRequest.get == RequestScope.ALL_TASKS_IN_CONTAINER
}
