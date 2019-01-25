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
package org.apache.samza.clustermanager;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.Test;


public class TestStandbyAllocator {

  @Test
  public void testWithNoStandby() {
    JobModel jobModel = getJobModelWithStandby(1, 1, 1);
    List<String> containerConstraints = StandbyTaskUtil.getStandbyContainerConstraints("0", jobModel);
    Assert.assertEquals("Constrained container count should be 0", 0, containerConstraints.size());
  }

  @Test
  public void testWithStandby() {
    testWithStandby(2, 1, 2);
    testWithStandby(10, 1, 2);

    testWithStandby(2, 10, 2);
    testWithStandby(2, 10, 4);

    testWithStandby(10, 1, 4);
    testWithStandby(10, 10, 4);
  }


  public void testWithStandby(int nContainers, int nTasks, int replicationFactor) {
    JobModel jobModel = getJobModelWithStandby(nContainers, nTasks, replicationFactor);

    for (String containerID : jobModel.getContainers().keySet()) {
      List<String> containerConstraints = StandbyTaskUtil.getStandbyContainerConstraints(containerID, jobModel);

      Assert.assertTrue("Constrained container should be valid containers",
          jobModel.getContainers().keySet().containsAll(containerConstraints));

      Assert.assertEquals("Constrained container count should be (replication factor-1)", replicationFactor - 1,
          containerConstraints.size());

      Assert.assertFalse("Constrained containers list should not have the container itself",
          containerConstraints.contains(containerID));

      containerConstraints.forEach(containerConstraintID -> {
          Assert.assertTrue("Constrained containers IDs should correspond to the active container",
              containerID.split("-")[0].equals(containerConstraintID.split("-")[0]));
        });
    }
  }

  // Helper method to create a jobmodel with given number of containers, tasks and replication factor
  private JobModel getJobModelWithStandby(int nContainers, int nTasks, int replicationFactor) {
    Map<String, ContainerModel> containerModels = new HashMap<>();
    int taskID = 0;

    for (int j = 0; j < nContainers; j++) {
      Map<TaskName, TaskModel> tasks = new HashMap<>();
      for (int i = 0; i < nTasks; i++) {
        TaskModel taskModel = getTaskModel(taskID++);
        tasks.put(taskModel.getTaskName(), taskModel);
      }
      containerModels.put(String.valueOf(j), new ContainerModel(String.valueOf(j), tasks));
    }

    Map<String, ContainerModel> standbyContainerModels = new HashMap<>();
    for (int i = 0; i < replicationFactor - 1; i++) {
      for (String containerID : containerModels.keySet()) {
        String standbyContainerId = StandbyTaskUtil.getStandbyContainerId(containerID, i);
        Map<TaskName, TaskModel> standbyTasks = getStandbyTasks(containerModels.get(containerID).getTasks(), i);
        standbyContainerModels.put(standbyContainerId, new ContainerModel(standbyContainerId, standbyTasks));
      }
    }

    containerModels.putAll(standbyContainerModels);
    return new JobModel(new MapConfig(), containerModels);
  }

  // Helper method that creates a taskmodel with one input ssp
  private static TaskModel getTaskModel(int partitionNum) {
    return new TaskModel(new TaskName("Partition " + partitionNum),
        Collections.singleton(new SystemStreamPartition("test-system", "test-stream", new Partition(partitionNum))),
        new Partition(partitionNum), TaskMode.Active);
  }

  // Helper method to create standby-taskModels from active-taskModels
  private Map<TaskName, TaskModel> getStandbyTasks(Map<TaskName, TaskModel> tasks, int replicaNum) {
    Map<TaskName, TaskModel> standbyTasks = new HashMap<>();
    tasks.forEach((taskName, taskModel) -> {
        TaskName standbyTaskName = StandbyTaskUtil.getStandbyTaskName(taskName, replicaNum);
        standbyTasks.put(standbyTaskName,
            new TaskModel(standbyTaskName, taskModel.getSystemStreamPartitions(), taskModel.getChangelogPartition(),
                TaskMode.Standby));
      });
    return standbyTasks;
  }
}
