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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


public class TestStandbyTaskGenerator {
  private StandbyEnabledTaskNameGrouper standbyTaskGenerator;

  @Test
  public void testBuddyContainerBasedGenerationIdentity() {
    this.standbyTaskGenerator = new StandbyEnabledTaskNameGrouper(Mockito.mock(TaskNameGrouper.class), true, 2);

    Assert.assertEquals("Shouldnt add standby tasks to empty container map", Collections.emptySet(),
        this.standbyTaskGenerator.generateStandbyTasks(Collections.emptySet(), 1));

    Assert.assertEquals("Shouldnt add standby tasks when repl factor = 1", getContainerMap(),
        this.standbyTaskGenerator.generateStandbyTasks(getContainerMap(), 1));
  }

  @Test
  public void testBuddyContainerBasedGenerationForVaryingRF() {
    testBuddyContainerBasedGeneration(1);
    testBuddyContainerBasedGeneration(2);
    testBuddyContainerBasedGeneration(3);
  }

  private void testBuddyContainerBasedGeneration(int replicationFactor) {
    this.standbyTaskGenerator = new StandbyEnabledTaskNameGrouper(Mockito.mock(TaskNameGrouper.class), true, 2);

    Set<ContainerModel> initialContainerModels = getContainerMap();
    Set<ContainerModel> containerModels =
        standbyTaskGenerator.generateStandbyTasks(initialContainerModels, replicationFactor);

    Assert.assertEquals(
        "The generated map should the required number of containers because repl fac = " + replicationFactor,
        initialContainerModels.size() * replicationFactor, containerModels.size());

    Assert.assertTrue("The generated map should have all active containers present as is",
        containerModels.containsAll(initialContainerModels));

    Assert.assertEquals(
        "The generated map should have the required total number of tasks because repl fac = " + replicationFactor,
        replicationFactor * initialContainerModels.stream().mapToInt(x -> x.getTasks().size()).sum(),
        containerModels.stream().mapToInt(x -> x.getTasks().size()).sum());

    int numActiveTasks = containerModels.stream()
        .mapToInt(container -> container.getTasks()
            .keySet()
            .stream()
            .filter(taskName -> !taskName.getTaskName().contains("Standby"))
            .toArray().length)
        .sum();

    int numStandbyTasks = containerModels.stream()
        .mapToInt(container -> container.getTasks()
            .keySet()
            .stream()
            .filter(taskName -> taskName.getTaskName().contains("Standby"))
            .toArray().length)
        .sum();

    Assert.assertEquals("The generated map should have the same number of active tasks as in the initial map",
        numActiveTasks, initialContainerModels.stream().mapToInt(x -> x.getTasks().size()).sum());

    Assert.assertEquals("The generated map should have numActive * (repl-1) standby tasks",
        numActiveTasks * (replicationFactor - 1), numStandbyTasks);
  }

  // Create a container map with two tasks in Container 0 and Container 1, and one in Container 2.
  private static Set<ContainerModel> getContainerMap() {
    Set<ContainerModel> retVal = new HashSet<>();

    Map<TaskName, TaskModel> tasksForContainer0 = new HashMap<>();
    tasksForContainer0.put(new TaskName("Partition 0"), getTaskModel(0));
    tasksForContainer0.put(new TaskName("Partition 1"), getTaskModel(1));
    retVal.add(new ContainerModel("0", tasksForContainer0));

    Map<TaskName, TaskModel> tasksForContainer1 = new HashMap<>();
    tasksForContainer1.put(new TaskName("Partition 2"), getTaskModel(2));
    tasksForContainer1.put(new TaskName("Partition 3"), getTaskModel(3));
    retVal.add(new ContainerModel("1", tasksForContainer1));

    retVal.add(new ContainerModel("2", Collections.singletonMap(new TaskName("Partition 4"), getTaskModel(4))));
    return retVal;
  }

  // Helper method that creates a taskmodel with one input ssp
  private static TaskModel getTaskModel(int partitionNum) {
    return new TaskModel(new TaskName("Partition " + partitionNum),
        Collections.singleton(new SystemStreamPartition("test-system", "test-stream", new Partition(partitionNum))),
        new Partition(partitionNum));
  }
}
