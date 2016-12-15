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
import java.util.concurrent.CopyOnWriteArraySet;
import org.apache.samza.container.TaskName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TaskCoordinatorRequests is used in run loop to collect the coordinator
 * requests from tasks, including commit requests and shutdown requests.
 * It is thread safe so it can be updated from multiple task threads.
 */
public class CoordinatorRequests {
  private static final Logger log = LoggerFactory.getLogger(CoordinatorRequests.class);

  private final Set<TaskName> taskNames;
  private final Set<TaskName> taskShutdownRequests = new CopyOnWriteArraySet<>();
  private final Set<TaskName> taskCommitRequests = new CopyOnWriteArraySet<>();
  volatile private boolean shutdownNow = false;

  public CoordinatorRequests(Set<TaskName> taskNames) {
    this.taskNames = taskNames;
  }

  public void update(ReadableCoordinator coordinator) {
    if (coordinator.commitRequest().isDefined() || coordinator.shutdownRequest().isDefined()) {
      checkCoordinator(coordinator);
    }
  }

  public Set<TaskName> commitRequests() {
    return taskCommitRequests;
  }

  public boolean shouldShutdownNow() {
    return shutdownNow;
  }

  /**
   * A new TaskCoordinator object is passed to a task on every call to StreamTask.process
   * and WindowableTask.window. This method checks whether the task requested that we
   * do something that affects the run loop (such as commit or shut down), and updates
   * run loop state accordingly.
   */
  private void checkCoordinator(ReadableCoordinator coordinator) {
    if (coordinator.requestedCommitTask()) {
      log.debug("Task {} requested commit for current task only", coordinator.taskName());
      taskCommitRequests.add(coordinator.taskName());
    }

    if (coordinator.requestedCommitAll()) {
      log.debug("Task {} requested commit for all tasks in the container", coordinator.taskName());
      taskCommitRequests.addAll(taskNames);
    }

    if (coordinator.requestedShutdownOnConsensus()) {
      taskShutdownRequests.add(coordinator.taskName());
      log.info("Shutdown has now been requested by tasks {}", taskShutdownRequests);
    }

    if (coordinator.requestedShutdownNow() || taskShutdownRequests.size() == taskNames.size()) {
      log.info("Shutdown requested.");
      shutdownNow = true;
    }
  }
}
