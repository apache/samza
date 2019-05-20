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
package org.apache.samza.job.model;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestJobModel {
  private final Config config;
  private final Map<String, ContainerModel> containers;
  private final Set<SystemStreamPartition> ssps1;
  private final Set<SystemStreamPartition> ssps2;
  private final Set<SystemStreamPartition> ssps3;
  private final Set<SystemStreamPartition> ssps4;
  private final Set<SystemStreamPartition> ssps5;

  public TestJobModel() {
    config = new MapConfig(ImmutableMap.of("a", "b"));
    ssps1 = ImmutableSet.of(
        new SystemStreamPartition("system1", "stream1_1", new Partition(0)),
        new SystemStreamPartition("system1", "stream1_2", new Partition(0)));
    ssps2 = ImmutableSet.of(
        new SystemStreamPartition("system1", "stream2_1", new Partition(1)),
        new SystemStreamPartition("system1", "stream2_2", new Partition(1)));
    ssps3 = ImmutableSet.of(
        new SystemStreamPartition("system1", "stream3_1", new Partition(2)),
        new SystemStreamPartition("system1", "stream3_2", new Partition(2)));
    ssps4 = ImmutableSet.of(
        new SystemStreamPartition("system1", "stream4_1", new Partition(3)),
        new SystemStreamPartition("system1", "stream4_2", new Partition(3)));
    ssps5 = ImmutableSet.of(
        new SystemStreamPartition("system1", "stream5_1", new Partition(4)),
        new SystemStreamPartition("system1", "stream5_2", new Partition(4)));
    Map<TaskName, TaskModel> tasksForContainer1 = ImmutableMap.of(
        new TaskName("t1"), new TaskModel(new TaskName("t1"), ssps1, new Partition(0)),
        new TaskName("t2"), new TaskModel(new TaskName("t2"), ssps2, new Partition(1)));
    Map<TaskName, TaskModel> tasksForContainer2 = ImmutableMap.of(
        new TaskName("t3"), new TaskModel(new TaskName("t3"), ssps3, new Partition(2)),
        new TaskName("t4"), new TaskModel(new TaskName("t4"), ssps4, new Partition(3)),
        new TaskName("t5"), new TaskModel(new TaskName("t5"), ssps5, new Partition(4)));
    ContainerModel containerModel1 = new ContainerModel("0", tasksForContainer1);
    ContainerModel containerModel2 = new ContainerModel("1", tasksForContainer2);
    containers = ImmutableMap.of("0", containerModel1, "1", containerModel2);
  }

  @Test
  public void testMaxChangeLogStreamPartitions() {
    JobModel jobModel = new JobModel(config, containers);
    assertEquals(5, jobModel.maxChangeLogStreamPartitions);
  }

  @Test
  public void testTaskToSystemStreamPartitions() {
    JobModel jobModel = new JobModel(config, containers);
    assertEquals(ssps1.size(), jobModel.getTaskToSystemStreamPartitions().get(new TaskName("t1")).size());
    assertTrue(jobModel.getTaskToSystemStreamPartitions().get(new TaskName("t1")).containsAll(ssps1));
    assertEquals(ssps2.size(), jobModel.getTaskToSystemStreamPartitions().get(new TaskName("t2")).size());
    assertTrue(jobModel.getTaskToSystemStreamPartitions().get(new TaskName("t2")).containsAll(ssps2));
    assertEquals(ssps3.size(), jobModel.getTaskToSystemStreamPartitions().get(new TaskName("t3")).size());
    assertTrue(jobModel.getTaskToSystemStreamPartitions().get(new TaskName("t3")).containsAll(ssps3));
    assertEquals(ssps4.size(), jobModel.getTaskToSystemStreamPartitions().get(new TaskName("t4")).size());
    assertTrue(jobModel.getTaskToSystemStreamPartitions().get(new TaskName("t4")).containsAll(ssps4));
    assertEquals(ssps5.size(), jobModel.getTaskToSystemStreamPartitions().get(new TaskName("t5")).size());
    assertTrue(jobModel.getTaskToSystemStreamPartitions().get(new TaskName("t5")).containsAll(ssps5));
  }
}