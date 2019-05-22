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
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestJobModelUtil {

  @Test(expected = IllegalArgumentException.class)
  public void testNullJobModel() {
    new JobModelUtil(null);
  }

  @Test
  public void testTaskToSystemStreamPartitions() {
    MapConfig config = new MapConfig(ImmutableMap.of("a", "b"));
    Set<SystemStreamPartition> ssps1 = ImmutableSet.of(
        new SystemStreamPartition("system1", "stream1_1", new Partition(0)),
        new SystemStreamPartition("system1", "stream1_2", new Partition(0)));
    Set<SystemStreamPartition> ssps2 = ImmutableSet.of(
        new SystemStreamPartition("system1", "stream2_1", new Partition(1)),
        new SystemStreamPartition("system1", "stream2_2", new Partition(1)));
    Set<SystemStreamPartition> ssps3 = ImmutableSet.of(
        new SystemStreamPartition("system1", "stream3_1", new Partition(2)),
        new SystemStreamPartition("system1", "stream3_2", new Partition(2)));
    Map<TaskName, TaskModel> tasksForContainer1 = ImmutableMap.of(
        new TaskName("t1"), new TaskModel(new TaskName("t1"), ssps1, new Partition(0)),
        new TaskName("t2"), new TaskModel(new TaskName("t2"), ssps2, new Partition(1)));
    Map<TaskName, TaskModel> tasksForContainer2 = ImmutableMap.of(
        new TaskName("t3"), new TaskModel(new TaskName("t3"), ssps3, new Partition(2)));
    ContainerModel containerModel1 = new ContainerModel("0", tasksForContainer1);
    ContainerModel containerModel2 = new ContainerModel("1", tasksForContainer2);
    Map<String, ContainerModel> containers = ImmutableMap.of("0", containerModel1, "1", containerModel2);

    JobModelUtil jobModelUtil = new JobModelUtil(new JobModel(config, containers));
    assertEquals(ssps1.size(), jobModelUtil.getTaskToSystemStreamPartitions().get(new TaskName("t1")).size());
    assertTrue(jobModelUtil.getTaskToSystemStreamPartitions().get(new TaskName("t1")).containsAll(ssps1));
    assertEquals(ssps2.size(), jobModelUtil.getTaskToSystemStreamPartitions().get(new TaskName("t2")).size());
    assertTrue(jobModelUtil.getTaskToSystemStreamPartitions().get(new TaskName("t2")).containsAll(ssps2));
    assertEquals(ssps3.size(), jobModelUtil.getTaskToSystemStreamPartitions().get(new TaskName("t3")).size());
    assertTrue(jobModelUtil.getTaskToSystemStreamPartitions().get(new TaskName("t3")).containsAll(ssps3));
  }
}
