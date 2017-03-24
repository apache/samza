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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.junit.Before;
import org.junit.Test;

import static org.apache.samza.container.mock.ContainerMocks.generateTaskModels;
import static org.apache.samza.container.mock.ContainerMocks.getTaskModel;
import static org.apache.samza.container.mock.ContainerMocks.getTaskName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestGroupByContainerIds {
  private TaskAssignmentManager taskAssignmentManager;
  private LocalityManager localityManager;

  @Before
  public void setup() {
    taskAssignmentManager = mock(TaskAssignmentManager.class);
    localityManager = mock(LocalityManager.class);
    when(localityManager.getTaskAssignmentManager()).thenReturn(taskAssignmentManager);


  }

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

  @Test(expected = IllegalArgumentException.class)
  public void testGroupFewerTasksThanContainers() {
    Set<TaskModel> taskModels = new HashSet<>();
    taskModels.add(getTaskModel(1));
    buildSimpleGrouper(2).group(taskModels);
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
      containersMap.put(container.getProcessorId(), container);
    }

    assertEquals(2, containers.size());
    ContainerModel container0 = containersMap.get(0);
    ContainerModel container1 = containersMap.get(1);
    assertNotNull(container0);
    assertNotNull(container1);
    assertEquals(0, container0.getProcessorId());
    assertEquals(1, container1.getProcessorId());
    assertEquals(3, container0.getTasks().size());
    assertEquals(2, container1.getTasks().size());
    assertTrue(container0.getTasks().containsKey(getTaskName(0)));
    assertTrue(container0.getTasks().containsKey(getTaskName(2)));
    assertTrue(container0.getTasks().containsKey(getTaskName(4)));
    assertTrue(container1.getTasks().containsKey(getTaskName(1)));
    assertTrue(container1.getTasks().containsKey(getTaskName(3)));
  }

  @Test
  public void testGroupHappyPathWithListOfContainers() {
    Set<TaskModel> taskModels = generateTaskModels(5);

    List<Integer> containerIds = new ArrayList<Integer>() {
      {
        add(4);
        add(2);
      }
    };

    Set<ContainerModel> containers = buildSimpleGrouper().group(taskModels, containerIds);

    Map<String, ContainerModel> containersMap = new HashMap<>();
    for (ContainerModel container : containers) {
      containersMap.put(container.getProcessorId(), container);
    }

    assertEquals(2, containers.size());
    ContainerModel container0 = containersMap.get(4);
    ContainerModel container1 = containersMap.get(2);
    assertNotNull(container0);
    assertNotNull(container1);
    assertEquals(4, container0.getProcessorId());
    assertEquals(2, container1.getProcessorId());
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

    List<Integer> containerIds = new ArrayList<Integer>() {
      {
        add(4);
        add(2);
      }
    };


    Set<ContainerModel> containers = buildSimpleGrouper().group(taskModels, containerIds);

    Map<String, ContainerModel> containersMap = new HashMap<>();
    for (ContainerModel container : containers) {
      containersMap.put(container.getProcessorId(), container);
    }

    assertEquals(2, containers.size());
    ContainerModel container0 = containersMap.get(4);
    ContainerModel container1 = containersMap.get(2);
    assertNotNull(container0);
    assertNotNull(container1);
    assertEquals(4, container0.getProcessorId());
    assertEquals(2, container1.getProcessorId());
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
}
