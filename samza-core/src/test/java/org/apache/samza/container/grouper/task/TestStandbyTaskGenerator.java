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
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.container.BuddyContainerBasedStandbyTaskGenerator;
import org.apache.samza.container.StandbyTaskGenerator;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.Test;


public class TestStandbyTaskGenerator {
  private StandbyTaskGenerator standbyTaskGenerator;

  @Test
  public void testBuddyContainerBasedGenerationIdentity() {
    this.standbyTaskGenerator = new BuddyContainerBasedStandbyTaskGenerator();

    Assert.assertEquals("Shouldnt add standby tasks to empty container map", Collections.emptyMap(),
        this.standbyTaskGenerator.generateStandbyTasks(Collections.emptyMap(), 1));

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
    this.standbyTaskGenerator = new BuddyContainerBasedStandbyTaskGenerator();

    Map<String, ContainerModel> initialContainerModelMap = getContainerMap();
    Map<String, ContainerModel> containerModelMap =
        standbyTaskGenerator.generateStandbyTasks(initialContainerModelMap, replicationFactor);

    Assert.assertEquals(
        "The generated map should the required number of containers because repl fac = " + replicationFactor,
        initialContainerModelMap.keySet().size() * replicationFactor, containerModelMap.keySet().size());

    Assert.assertTrue("The generated map should have all active containers present as is", containerModelMap.entrySet().containsAll(initialContainerModelMap.entrySet()));

    Assert.assertEquals(
        "The generated map should have the required total number of tasks because repl fac = " + replicationFactor,
        replicationFactor * initialContainerModelMap.values().stream().mapToInt(x -> x.getTasks().size()).sum(),
        containerModelMap.values().stream().mapToInt(x -> x.getTasks().size()).sum());

    int numActiveTasks = containerModelMap.values()
        .stream()
        .mapToInt(container -> container.getTasks()
            .keySet()
            .stream()
            .filter(taskName -> !taskName.getTaskName().contains("Standby"))
            .toArray().length)
        .sum();

    int numStandbyTasks = containerModelMap.values()
        .stream()
        .mapToInt(container -> container.getTasks()
            .keySet()
            .stream()
            .filter(taskName -> taskName.getTaskName().contains("Standby"))
            .toArray().length)
        .sum();

    Assert.assertEquals("The generated map should have the same number of active tasks as in the initial map",
        numActiveTasks, initialContainerModelMap.values().stream().mapToInt(x -> x.getTasks().size()).sum());

    Assert.assertEquals("The generated map should have numActive * (repl-1) standby tasks",
        numActiveTasks * (replicationFactor - 1), numStandbyTasks);
  }

  // Create a container map with two tasks in Container 0 and Container 1, and one in Container 2.
  private static Map<String, ContainerModel> getContainerMap() {
    Map<String, ContainerModel> retVal = new HashMap<>();

    Map<TaskName, TaskModel> tasksForContainer0 = new HashMap<>();
    tasksForContainer0.put(new TaskName("Partition 0"), getTaskModel(0));
    tasksForContainer0.put(new TaskName("Partition 1"), getTaskModel(1));
    retVal.put("0", new ContainerModel("0", tasksForContainer0));

    Map<TaskName, TaskModel> tasksForContainer1 = new HashMap<>();
    tasksForContainer1.put(new TaskName("Partition 2"), getTaskModel(2));
    tasksForContainer1.put(new TaskName("Partition 3"), getTaskModel(3));
    retVal.put("1", new ContainerModel("1", tasksForContainer1));

    retVal.put("2", new ContainerModel("2", Collections.singletonMap(new TaskName("Partition 4"), getTaskModel(4))));
    return retVal;
  }

  // Helper method that creates a taskmodel with one input ssp
  private static TaskModel getTaskModel(int partitionNum) {
    return new TaskModel(new TaskName("Partition " + partitionNum),
        Collections.singleton(new SystemStreamPartition("test-system", "test-stream", new Partition(partitionNum))),
        new Partition(partitionNum));
  }
}
