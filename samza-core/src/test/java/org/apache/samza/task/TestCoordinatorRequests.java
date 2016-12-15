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

import java.util.HashSet;
import java.util.Set;
import org.apache.samza.container.TaskName;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCoordinatorRequests {
  CoordinatorRequests coordinatorRequests;
  TaskName taskA = new TaskName("a");
  TaskName taskB = new TaskName("b");
  TaskName taskC = new TaskName("c");


  @Before
  public void setup() {
    Set<TaskName> taskNames = new HashSet<>();
    taskNames.add(taskA);
    taskNames.add(taskB);
    taskNames.add(taskC);

    coordinatorRequests = new CoordinatorRequests(taskNames);
  }

  @Test
  public void testUpdateCommit() {
    ReadableCoordinator coordinator = new ReadableCoordinator(taskA);
    coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    coordinatorRequests.update(coordinator);
    assertTrue(coordinatorRequests.commitRequests().contains(taskA));

    coordinator = new ReadableCoordinator(taskC);
    coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    coordinatorRequests.update(coordinator);
    assertTrue(coordinatorRequests.commitRequests().contains(taskC));
    assertFalse(coordinatorRequests.commitRequests().contains(taskB));
    assertTrue(coordinatorRequests.commitRequests().size() == 2);

    coordinator.commit(TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER);
    coordinatorRequests.update(coordinator);
    assertTrue(coordinatorRequests.commitRequests().contains(taskB));
    assertTrue(coordinatorRequests.commitRequests().size() == 3);
  }

  @Test
  public void testUpdateShutdownOnConsensus() {
    ReadableCoordinator coordinator = new ReadableCoordinator(taskA);
    coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
    coordinatorRequests.update(coordinator);
    assertFalse(coordinatorRequests.shouldShutdownNow());

    coordinator = new ReadableCoordinator(taskB);
    coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
    coordinatorRequests.update(coordinator);
    assertFalse(coordinatorRequests.shouldShutdownNow());

    coordinator = new ReadableCoordinator(taskC);
    coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
    coordinatorRequests.update(coordinator);
    assertTrue(coordinatorRequests.shouldShutdownNow());
  }

  @Test
  public void testUpdateShutdownNow() {
    ReadableCoordinator coordinator = new ReadableCoordinator(taskA);
    coordinator.shutdown(TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER);
    coordinatorRequests.update(coordinator);
    assertTrue(coordinatorRequests.shouldShutdownNow());
  }
}
