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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.runtime.LocationId;
import org.junit.Test;

import static org.apache.samza.container.mock.ContainerMocks.generateTaskModels;
import static org.apache.samza.container.mock.ContainerMocks.getTaskName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestGroupByContainerIds {

  private Config buildConfigForContainerCount(int count) {
    Map<String, String> map = new HashMap<>();
    map.put("job.container.count", String.valueOf(count));
    return new MapConfig(map);
  }

  private TaskNameGrouper buildSimpleGrouper() {
    return buildSimpleGrouper(1);
  }

  private TaskNameGrouper buildSimpleGrouper(int containerCount) {
    return new GroupByContainerIdsFactory().build(buildConfigForContainerCount(containerCount));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGroupEmptyTasks() {
    buildSimpleGrouper(1).group(new HashSet());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGrouperResultImmutable() {
    Set<TaskModel> taskModels = generateTaskModels(3);
    Set<ContainerModel> containers = buildSimpleGrouper(2).group(taskModels);
    containers.remove(containers.iterator().next());
  }

  @Test
  public void testGroupHappyPath() {
    Set<TaskModel> taskModels = generateTaskModels(5);

    Set<ContainerModel> containers = buildSimpleGrouper(2).group(taskModels);

    Map<String, ContainerModel> containersMap = new HashMap<>();
    for (ContainerModel container : containers) {
      containersMap.put(container.getId(), container);
    }

    assertEquals(2, containers.size());
    ContainerModel container0 = containersMap.get("0");
    ContainerModel container1 = containersMap.get("1");
    assertNotNull(container0);
    assertNotNull(container1);
    assertEquals("0", container0.getId());
    assertEquals("1", container1.getId());
    assertEquals(3, container0.getTasks().size());
    assertEquals(2, container1.getTasks().size());
    assertTrue(container0.getTasks().containsKey(getTaskName(0)));
    assertTrue(container0.getTasks().containsKey(getTaskName(2)));
    assertTrue(container0.getTasks().containsKey(getTaskName(4)));
    assertTrue(container1.getTasks().containsKey(getTaskName(1)));
    assertTrue(container1.getTasks().containsKey(getTaskName(3)));
  }

  @Test
  public void testGroupWithNullContainerIds() {
    Set<TaskModel> taskModels = generateTaskModels(5);

    List<String> containerIds = null;
    Set<ContainerModel> containers = buildSimpleGrouper(2).group(taskModels, containerIds);

    Map<String, ContainerModel> containersMap = new HashMap<>();
    for (ContainerModel container : containers) {
      containersMap.put(container.getId(), container);
    }

    assertEquals(2, containers.size());
    ContainerModel container0 = containersMap.get("0");
    ContainerModel container1 = containersMap.get("1");
    assertNotNull(container0);
    assertNotNull(container1);
    assertEquals("0", container0.getId());
    assertEquals("1", container1.getId());
    assertEquals(3, container0.getTasks().size());
    assertEquals(2, container1.getTasks().size());
    assertTrue(container0.getTasks().containsKey(getTaskName(0)));
    assertTrue(container0.getTasks().containsKey(getTaskName(2)));
    assertTrue(container0.getTasks().containsKey(getTaskName(4)));
    assertTrue(container1.getTasks().containsKey(getTaskName(1)));
    assertTrue(container1.getTasks().containsKey(getTaskName(3)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGroupWithEmptyContainerIds() {
    Set<TaskModel> taskModels = generateTaskModels(5);

    buildSimpleGrouper(2).group(taskModels, Collections.emptyList());
  }

  @Test
  public void testGroupHappyPathWithListOfContainers() {
    Set<TaskModel> taskModels = generateTaskModels(5);

    List<String> containerIds = new ArrayList<String>() {
      {
        add("4");
        add("2");
      }
    };

    Set<ContainerModel> containers = buildSimpleGrouper().group(taskModels, containerIds);

    Map<String, ContainerModel> containersMap = new HashMap<>();
    for (ContainerModel container : containers) {
      containersMap.put(container.getId(), container);
    }

    assertEquals(2, containers.size());
    ContainerModel container0 = containersMap.get("4");
    ContainerModel container1 = containersMap.get("2");
    assertNotNull(container0);
    assertNotNull(container1);
    assertEquals("4", container0.getId());
    assertEquals("2", container1.getId());
    assertEquals(3, container0.getTasks().size());
    assertEquals(2, container1.getTasks().size());
    assertTrue(container0.getTasks().containsKey(getTaskName(0)));
    assertTrue(container0.getTasks().containsKey(getTaskName(2)));
    assertTrue(container0.getTasks().containsKey(getTaskName(4)));
    assertTrue(container1.getTasks().containsKey(getTaskName(1)));
    assertTrue(container1.getTasks().containsKey(getTaskName(3)));
  }


  @Test
  public void testGroupManyTasks() {
    Set<TaskModel> taskModels = generateTaskModels(21);

    List<String> containerIds = new ArrayList<String>() {
      {
        add("4");
        add("2");
      }
    };


    Set<ContainerModel> containers = buildSimpleGrouper().group(taskModels, containerIds);

    Map<String, ContainerModel> containersMap = new HashMap<>();
    for (ContainerModel container : containers) {
      containersMap.put(container.getId(), container);
    }

    assertEquals(2, containers.size());
    ContainerModel container0 = containersMap.get("4");
    ContainerModel container1 = containersMap.get("2");
    assertNotNull(container0);
    assertNotNull(container1);
    assertEquals("4", container0.getId());
    assertEquals("2", container1.getId());
    assertEquals(11, container0.getTasks().size());
    assertEquals(10, container1.getTasks().size());

    // NOTE: tasks are sorted lexicographically, so the container assignment
    // can seem odd, but the consistency is the key focus
    assertTrue(container0.getTasks().containsKey(getTaskName(0)));
    assertTrue(container0.getTasks().containsKey(getTaskName(10)));
    assertTrue(container0.getTasks().containsKey(getTaskName(12)));
    assertTrue(container0.getTasks().containsKey(getTaskName(14)));
    assertTrue(container0.getTasks().containsKey(getTaskName(16)));
    assertTrue(container0.getTasks().containsKey(getTaskName(18)));
    assertTrue(container0.getTasks().containsKey(getTaskName(2)));
    assertTrue(container0.getTasks().containsKey(getTaskName(3)));
    assertTrue(container0.getTasks().containsKey(getTaskName(5)));
    assertTrue(container0.getTasks().containsKey(getTaskName(7)));
    assertTrue(container0.getTasks().containsKey(getTaskName(9)));

    assertTrue(container1.getTasks().containsKey(getTaskName(1)));
    assertTrue(container1.getTasks().containsKey(getTaskName(11)));
    assertTrue(container1.getTasks().containsKey(getTaskName(13)));
    assertTrue(container1.getTasks().containsKey(getTaskName(15)));
    assertTrue(container1.getTasks().containsKey(getTaskName(17)));
    assertTrue(container1.getTasks().containsKey(getTaskName(19)));
    assertTrue(container1.getTasks().containsKey(getTaskName(20)));
    assertTrue(container1.getTasks().containsKey(getTaskName(4)));
    assertTrue(container1.getTasks().containsKey(getTaskName(6)));
    assertTrue(container1.getTasks().containsKey(getTaskName(8)));
  }

  @Test
  public void testFewerTasksThanContainers() {
    final String testContainerId1 = "1";
    final String testContainerId2 = "2";

    Set<TaskModel> taskModels = generateTaskModels(1);
    List<String> containerIds = ImmutableList.of(testContainerId1, testContainerId2);

    Map<TaskName, TaskModel> expectedTasks = taskModels.stream()
                                                       .collect(Collectors.toMap(TaskModel::getTaskName, x -> x));
    ContainerModel expectedContainerModel = new ContainerModel(testContainerId1, expectedTasks);

    Set<ContainerModel> actualContainerModels = buildSimpleGrouper().group(taskModels, containerIds);

    assertEquals(1, actualContainerModels.size());
    assertEquals(ImmutableSet.of(expectedContainerModel), actualContainerModels);
  }

  @Test
  public void testShouldUseTaskLocalityWhenGeneratingContainerModels() {
    TaskNameGrouper taskNameGrouper = buildSimpleGrouper(3);

    String testProcessorId1 = "testProcessorId1";
    String testProcessorId2 = "testProcessorId2";
    String testProcessorId3 = "testProcessorId3";

    LocationId testLocationId1 = new LocationId("testLocationId1");
    LocationId testLocationId2 = new LocationId("testLocationId2");
    LocationId testLocationId3 = new LocationId("testLocationId3");

    TaskName testTaskName1 = new TaskName("testTasKId1");
    TaskName testTaskName2 = new TaskName("testTaskId2");
    TaskName testTaskName3 = new TaskName("testTaskId3");

    TaskModel testTaskModel1 = new TaskModel(testTaskName1, new HashSet<>(), new Partition(0));
    TaskModel testTaskModel2 = new TaskModel(testTaskName2, new HashSet<>(), new Partition(1));
    TaskModel testTaskModel3 = new TaskModel(testTaskName3, new HashSet<>(), new Partition(2));

    Map<String, LocationId> processorLocality = ImmutableMap.of(testProcessorId1, testLocationId1,
                                                                testProcessorId2, testLocationId2,
                                                                testProcessorId3, testLocationId3);

    Map<TaskName, LocationId> taskLocality = ImmutableMap.of(testTaskName1, testLocationId1,
                                                             testTaskName2, testLocationId2,
                                                             testTaskName3, testLocationId3);

    MetadataProviderImpl grouperContext = new MetadataProviderImpl(processorLocality, taskLocality, new HashMap<>(), new HashMap<>());

    Set<TaskModel> taskModels = ImmutableSet.of(testTaskModel1, testTaskModel2, testTaskModel3);

    Set<ContainerModel> expectedContainerModels = ImmutableSet.of(new ContainerModel(testProcessorId1, ImmutableMap.of(testTaskName1, testTaskModel1)),
                                                                  new ContainerModel(testProcessorId2, ImmutableMap.of(testTaskName2, testTaskModel2)),
                                                                  new ContainerModel(testProcessorId3, ImmutableMap.of(testTaskName3, testTaskModel3)));

    Set<ContainerModel> actualContainerModels = taskNameGrouper.group(taskModels, grouperContext);

    assertEquals(expectedContainerModels, actualContainerModels);
  }

  @Test
  public void testGenerateContainerModelForSingleContainer() {
    TaskNameGrouper taskNameGrouper = buildSimpleGrouper(1);

    String testProcessorId1 = "testProcessorId1";

    LocationId testLocationId1 = new LocationId("testLocationId1");
    LocationId testLocationId2 = new LocationId("testLocationId2");
    LocationId testLocationId3 = new LocationId("testLocationId3");

    TaskName testTaskName1 = new TaskName("testTasKId1");
    TaskName testTaskName2 = new TaskName("testTaskId2");
    TaskName testTaskName3 = new TaskName("testTaskId3");

    TaskModel testTaskModel1 = new TaskModel(testTaskName1, new HashSet<>(), new Partition(0));
    TaskModel testTaskModel2 = new TaskModel(testTaskName2, new HashSet<>(), new Partition(1));
    TaskModel testTaskModel3 = new TaskModel(testTaskName3, new HashSet<>(), new Partition(2));

    Map<String, LocationId> processorLocality = ImmutableMap.of(testProcessorId1, testLocationId1);

    Map<TaskName, LocationId> taskLocality = ImmutableMap.of(testTaskName1, testLocationId1,
                                                             testTaskName2, testLocationId2,
                                                             testTaskName3, testLocationId3);

    MetadataProviderImpl grouperContext = new MetadataProviderImpl(processorLocality, taskLocality, new HashMap<>(), new HashMap<>());

    Set<TaskModel> taskModels = ImmutableSet.of(testTaskModel1, testTaskModel2, testTaskModel3);

    Set<ContainerModel> expectedContainerModels = ImmutableSet.of(new ContainerModel(testProcessorId1, ImmutableMap.of(testTaskName1, testTaskModel1,
                                                                                                                       testTaskName2, testTaskModel2,
                                                                                                                       testTaskName3, testTaskModel3)));

    Set<ContainerModel> actualContainerModels = taskNameGrouper.group(taskModels, grouperContext);

    assertEquals(expectedContainerModels, actualContainerModels);
  }

  @Test
  public void testShouldGenerateCorrectContainerModelWhenTaskLocalityIsEmpty() {
    TaskNameGrouper taskNameGrouper = buildSimpleGrouper(3);

    String testProcessorId1 = "testProcessorId1";
    String testProcessorId2 = "testProcessorId2";
    String testProcessorId3 = "testProcessorId3";

    LocationId testLocationId1 = new LocationId("testLocationId1");
    LocationId testLocationId2 = new LocationId("testLocationId2");
    LocationId testLocationId3 = new LocationId("testLocationId3");

    TaskName testTaskName1 = new TaskName("testTasKId1");
    TaskName testTaskName2 = new TaskName("testTaskId2");
    TaskName testTaskName3 = new TaskName("testTaskId3");

    TaskModel testTaskModel1 = new TaskModel(testTaskName1, new HashSet<>(), new Partition(0));
    TaskModel testTaskModel2 = new TaskModel(testTaskName2, new HashSet<>(), new Partition(1));
    TaskModel testTaskModel3 = new TaskModel(testTaskName3, new HashSet<>(), new Partition(2));

    Map<String, LocationId> processorLocality = ImmutableMap.of(testProcessorId1, testLocationId1,
                                                                testProcessorId2, testLocationId2,
                                                                testProcessorId3, testLocationId3);

    Map<TaskName, LocationId> taskLocality = ImmutableMap.of(testTaskName1, testLocationId1);

    MetadataProviderImpl grouperContext = new MetadataProviderImpl(processorLocality, taskLocality, new HashMap<>(), new HashMap<>());

    Set<TaskModel> taskModels = ImmutableSet.of(testTaskModel1, testTaskModel2, testTaskModel3);

    Set<ContainerModel> expectedContainerModels = ImmutableSet.of(new ContainerModel(testProcessorId1, ImmutableMap.of(testTaskName1, testTaskModel1)),
                                                                  new ContainerModel(testProcessorId2, ImmutableMap.of(testTaskName2, testTaskModel2)),
                                                                  new ContainerModel(testProcessorId3, ImmutableMap.of(testTaskName3, testTaskModel3)));

    Set<ContainerModel> actualContainerModels = taskNameGrouper.group(taskModels, grouperContext);

    assertEquals(expectedContainerModels, actualContainerModels);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testShouldFailWhenProcessorLocalityIsEmpty() {
    TaskNameGrouper taskNameGrouper = buildSimpleGrouper(3);

    MetadataProviderImpl grouperContext = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>());

    taskNameGrouper.group(new HashSet<>(), grouperContext);
  }

  @Test
  public void testShouldGenerateIdenticalTaskDistributionWhenNoChangeInProcessorGroup() {
    TaskNameGrouper taskNameGrouper = buildSimpleGrouper(3);

    String testProcessorId1 = "testProcessorId1";
    String testProcessorId2 = "testProcessorId2";
    String testProcessorId3 = "testProcessorId3";

    LocationId testLocationId1 = new LocationId("testLocationId1");
    LocationId testLocationId2 = new LocationId("testLocationId2");
    LocationId testLocationId3 = new LocationId("testLocationId3");

    TaskName testTaskName1 = new TaskName("testTasKId1");
    TaskName testTaskName2 = new TaskName("testTaskId2");
    TaskName testTaskName3 = new TaskName("testTaskId3");

    TaskModel testTaskModel1 = new TaskModel(testTaskName1, new HashSet<>(), new Partition(0));
    TaskModel testTaskModel2 = new TaskModel(testTaskName2, new HashSet<>(), new Partition(1));
    TaskModel testTaskModel3 = new TaskModel(testTaskName3, new HashSet<>(), new Partition(2));

    Map<String, LocationId> processorLocality = ImmutableMap.of(testProcessorId1, testLocationId1,
            testProcessorId2, testLocationId2,
            testProcessorId3, testLocationId3);

    Map<TaskName, LocationId> taskLocality = ImmutableMap.of(testTaskName1, testLocationId1,
            testTaskName2, testLocationId2,
            testTaskName3, testLocationId3);

    MetadataProviderImpl grouperContext = new MetadataProviderImpl(processorLocality, taskLocality, new HashMap<>(), new HashMap<>());

    Set<TaskModel> taskModels = ImmutableSet.of(testTaskModel1, testTaskModel2, testTaskModel3);

    Set<ContainerModel> expectedContainerModels = ImmutableSet.of(new ContainerModel(testProcessorId1, ImmutableMap.of(testTaskName1, testTaskModel1)),
            new ContainerModel(testProcessorId2, ImmutableMap.of(testTaskName2, testTaskModel2)),
            new ContainerModel(testProcessorId3, ImmutableMap.of(testTaskName3, testTaskModel3)));

    Set<ContainerModel> actualContainerModels = taskNameGrouper.group(taskModels, grouperContext);

    assertEquals(expectedContainerModels, actualContainerModels);

    actualContainerModels = taskNameGrouper.group(taskModels, grouperContext);

    assertEquals(expectedContainerModels, actualContainerModels);
  }

  @Test
  public void testShouldMinimizeTaskShuffleWhenAvailableProcessorInGroupChanges() {
    TaskNameGrouper taskNameGrouper = buildSimpleGrouper(3);

    String testProcessorId1 = "testProcessorId1";
    String testProcessorId2 = "testProcessorId2";
    String testProcessorId3 = "testProcessorId3";

    LocationId testLocationId1 = new LocationId("testLocationId1");
    LocationId testLocationId2 = new LocationId("testLocationId2");
    LocationId testLocationId3 = new LocationId("testLocationId3");

    TaskName testTaskName1 = new TaskName("testTasKId1");
    TaskName testTaskName2 = new TaskName("testTaskId2");
    TaskName testTaskName3 = new TaskName("testTaskId3");

    TaskModel testTaskModel1 = new TaskModel(testTaskName1, new HashSet<>(), new Partition(0));
    TaskModel testTaskModel2 = new TaskModel(testTaskName2, new HashSet<>(), new Partition(1));
    TaskModel testTaskModel3 = new TaskModel(testTaskName3, new HashSet<>(), new Partition(2));

    Map<String, LocationId> processorLocality = ImmutableMap.of(testProcessorId1, testLocationId1,
            testProcessorId2, testLocationId2,
            testProcessorId3, testLocationId3);

    Map<TaskName, LocationId> taskLocality = ImmutableMap.of(testTaskName1, testLocationId1,
            testTaskName2, testLocationId2,
            testTaskName3, testLocationId3);

    MetadataProviderImpl grouperContext = new MetadataProviderImpl(processorLocality, taskLocality, new HashMap<>(), new HashMap<>());

    Set<TaskModel> taskModels = ImmutableSet.of(testTaskModel1, testTaskModel2, testTaskModel3);

    Set<ContainerModel> expectedContainerModels = ImmutableSet.of(new ContainerModel(testProcessorId1, ImmutableMap.of(testTaskName1, testTaskModel1)),
            new ContainerModel(testProcessorId2, ImmutableMap.of(testTaskName2, testTaskModel2)),
            new ContainerModel(testProcessorId3, ImmutableMap.of(testTaskName3, testTaskModel3)));

    Set<ContainerModel> actualContainerModels = taskNameGrouper.group(taskModels, grouperContext);

    assertEquals(expectedContainerModels, actualContainerModels);

    processorLocality = ImmutableMap.of(testProcessorId1, testLocationId1,
                                        testProcessorId2, testLocationId2);

    grouperContext = new MetadataProviderImpl(processorLocality, taskLocality, new HashMap<>(), new HashMap<>());

    actualContainerModels = taskNameGrouper.group(taskModels, grouperContext);

    expectedContainerModels = ImmutableSet.of(new ContainerModel(testProcessorId1, ImmutableMap.of(testTaskName1, testTaskModel1, testTaskName3, testTaskModel3)),
                                              new ContainerModel(testProcessorId2, ImmutableMap.of(testTaskName2, testTaskModel2)));

    assertEquals(expectedContainerModels, actualContainerModels);
  }
}
