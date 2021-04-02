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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class TestJobModelUtil {
  private static final String PROCESSOR_ID = "testProcessor";

  @Test
  public void testCompareContainerModels() {
    final ContainerModel mockContainerModel = mock(ContainerModel.class);
    final JobModel first = mock(JobModel.class);
    final JobModel second = mock(JobModel.class);
    final String testProcessor2 = "testProcessor2";

    when(first.getContainers()).thenReturn(ImmutableMap.of(PROCESSOR_ID, mockContainerModel));
    when(second.getContainers()).thenReturn(ImmutableMap.of(PROCESSOR_ID, mockContainerModel));

    assertTrue("Expecting null job models to return true", JobModelUtil.compareContainerModels(null, null));
    assertTrue("Expecting true for job model with same container model",
        JobModelUtil.compareContainerModels(first, second));

    when(second.getContainers()).thenReturn(ImmutableMap.of(PROCESSOR_ID, mock(ContainerModel.class)));
    assertFalse("Expecting false for two different job model",
        JobModelUtil.compareContainerModels(first, second));

    when(second.getContainers()).thenReturn(ImmutableMap.of(PROCESSOR_ID, mockContainerModel, testProcessor2,
        mock(ContainerModel.class)));
    assertFalse("Expecting false for two different job model",
        JobModelUtil.compareContainerModels(first, second));

  }

  @Test(expected = IllegalArgumentException.class)
  public void testCompareContainerModelForNullProcessor() {
    JobModelUtil.compareContainerModelForProcessor(null, mock(JobModel.class), mock(JobModel.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCompareContainerModelForBlankProcessor() {
    JobModelUtil.compareContainerModelForProcessor("", mock(JobModel.class), mock(JobModel.class));
  }

  /**
   * Tests the following scenarios as part of the same tests to reduce boiler plate and potentially allow
   * parallel executions of tests for the class.
   *  1. Test null job models for processors
   *  2. Test same container models across job models for processors
   *  3. Test different container models across job models for processors
   *  4. Test absence of container model vs presence across job models for processors
   *
   * The approach below leans towards performance (parallel execution) as opposed to readability; i.e sharing setup of
   * the tests and having multiple tests that test individual scenarios. Additionally, the individual scenarios being
   * tested are self explanatory.
   */
  @Test
  public void testCompareContainerModelForProcessor() {
    final JobModel firstJobModel = mock(JobModel.class);
    final JobModel secondJobModel = mock(JobModel.class);
    final ContainerModel mockContainerModel = mock(ContainerModel.class);
    Map<String, ContainerModel> mockContainerModels = mock(Map.class);

    assertTrue("Null job models should return true for comparison",
        JobModelUtil.compareContainerModelForProcessor(PROCESSOR_ID, null, null));

    when(firstJobModel.getContainers()).thenReturn(mockContainerModels);
    when(secondJobModel.getContainers()).thenReturn(mockContainerModels);
    when(mockContainerModels.get(PROCESSOR_ID)).thenReturn(mockContainerModel);
    assertTrue("Expecting both job model to have same container model for the processor",
        JobModelUtil.compareContainerModelForProcessor(PROCESSOR_ID, firstJobModel, secondJobModel));

    when(mockContainerModels.get(PROCESSOR_ID))
        .thenReturn(mockContainerModel)
        .thenReturn(mock(ContainerModel.class));
    assertFalse("Expecting container models to be different across job models for the processor",
        JobModelUtil.compareContainerModelForProcessor(PROCESSOR_ID, firstJobModel, secondJobModel));

    when(mockContainerModels.get(PROCESSOR_ID)).thenReturn(null)
        .thenReturn(mockContainerModel);
    assertFalse("Expecting container models to be different across job models for the processor",
        JobModelUtil.compareContainerModelForProcessor(PROCESSOR_ID, firstJobModel, secondJobModel));
  }

  @Test
  public void testGetTaskNamesForProcessorAbsentInJobModel() {
    JobModel mockJobModel = mock(JobModel.class);
    when(mockJobModel.getContainers()).thenReturn(mock(Map.class));

    Set<TaskName> taskNames = JobModelUtil.getTaskNamesForProcessor("testProcessor", mockJobModel);
    assertTrue("TaskNames should be empty", taskNames.isEmpty());
  }

  @Test
  public void testGetTaskNamesForProcessorPresentInJobModel() {
    TaskName expectedTaskName = new TaskName("testTaskName");
    String processorId = "testProcessor";
    JobModel mockJobModel = mock(JobModel.class);
    ContainerModel mockContainerModel = mock(ContainerModel.class);
    Map<String, ContainerModel> mockContainers = mock(Map.class);

    when(mockContainers.get(processorId)).thenReturn(mockContainerModel);
    when(mockContainerModel.getTasks()).thenReturn(ImmutableMap.of(expectedTaskName, mock(TaskModel.class)));
    when(mockJobModel.getContainers()).thenReturn(mockContainers);

    Set<TaskName> actualTaskNames = JobModelUtil.getTaskNamesForProcessor(processorId, mockJobModel);
    assertEquals("Expecting TaskNames size = 1", 1, actualTaskNames.size());
    assertTrue("Expecting testTaskName to be returned", actualTaskNames.contains(expectedTaskName));
  }

  @Test(expected = NullPointerException.class)
  public void testGetTaskNamesForProcessorWithNullJobModel() {
    JobModelUtil.getTaskNamesForProcessor("processor", null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetTaskNamesForProcessorWithEmptyProcessorId() {
    JobModelUtil.getTaskNamesForProcessor("", mock(JobModel.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetTaskNamesForProcessorWithNullProcessorId() {
    JobModelUtil.getTaskNamesForProcessor(null, mock(JobModel.class));
  }

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
}
