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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

public class TestJobModelUtil {

  @Test(expected = IllegalArgumentException.class)
  public void testTaskToSystemStreamPartitionsWithNullJobModel() {
    JobModelUtil.getTaskToSystemStreamPartitions(null);
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
    Set<SystemStreamPartition> ssps4 = ImmutableSet.of(
        new SystemStreamPartition("system1", "stream4_1", new Partition(3)),
        new SystemStreamPartition("system1", "stream4_2", new Partition(3)));
    TaskName task1 = new TaskName("task1"); // adding this task to both container models
    TaskName task2 = new TaskName("task2");
    Map<TaskName, TaskModel> tasksForContainer1 = ImmutableMap.of(
        task1, new TaskModel(task1, ssps1, new Partition(0)),
        task2, new TaskModel(task2, ssps2, new Partition(1)));
    TaskName task3 = new TaskName("task3");
    Map<TaskName, TaskModel> tasksForContainer2 = ImmutableMap.of(
        task3, new TaskModel(task3, ssps3, new Partition(2)),
        task1, new TaskModel(task1, ssps4, new Partition(3)));
    ContainerModel containerModel1 = new ContainerModel("0", tasksForContainer1);
    ContainerModel containerModel2 = new ContainerModel("1", tasksForContainer2);
    Map<String, ContainerModel> containers = ImmutableMap.of("0", containerModel1, "1", containerModel2);

    JobModel jobModel = new JobModel(config, containers);

    // test having same task1 in multiple containers
    assertEquals(ssps1.size() + ssps4.size(), JobModelUtil.getTaskToSystemStreamPartitions(jobModel).get(task1).size());
    assertTrue(JobModelUtil.getTaskToSystemStreamPartitions(jobModel).get(task1).containsAll(ssps1));
    assertTrue(JobModelUtil.getTaskToSystemStreamPartitions(jobModel).get(task1).containsAll(ssps4));

    assertEquals(ssps2.size(), JobModelUtil.getTaskToSystemStreamPartitions(jobModel).get(task2).size());
    assertTrue(JobModelUtil.getTaskToSystemStreamPartitions(jobModel).get(task2).containsAll(ssps2));

    assertEquals(ssps3.size(), JobModelUtil.getTaskToSystemStreamPartitions(jobModel).get(task3).size());
    assertTrue(JobModelUtil.getTaskToSystemStreamPartitions(jobModel).get(task3).containsAll(ssps3));
  }

  @Test
  public void testReadJobModelReturnResultFromMetadataStore() throws IOException {
    MetadataStore mockMetadataStore = Mockito.mock(MetadataStore.class);
    JobModel testModel = new JobModel(new MapConfig(), new HashMap<>());
    byte[] testValue = SamzaObjectMapper.getObjectMapper().writeValueAsBytes(testModel);

    Mockito.when(mockMetadataStore.get("jobModelGeneration/jobModels/1")).thenReturn(null);
    Mockito.when(mockMetadataStore.get("jobModelGeneration/jobModels/2")).thenReturn(testValue);

    assertNull(JobModelUtil.readJobModel("1", mockMetadataStore));
    assertEquals(testModel, JobModelUtil.readJobModel("2", mockMetadataStore));
  }
}
