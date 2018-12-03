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
import java.util.UUID;

import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.SamzaException;
import org.junit.Test;

import static org.apache.samza.container.mock.ContainerMocks.*;
import static org.junit.Assert.*;

public class TestGroupByContainerCount {

  @Test(expected = IllegalArgumentException.class)
  public void testGroupEmptyTasks() {
    new GroupByContainerCount(1).group(new HashSet<>());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGroupFewerTasksThanContainers() {
    Set<TaskModel> taskModels = new HashSet<>();
    taskModels.add(getTaskModel(1));
    new GroupByContainerCount(2).group(taskModels);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGrouperResultImmutable() {
    Set<TaskModel> taskModels = generateTaskModels(3);
    Set<ContainerModel> containers = new GroupByContainerCount(3).group(taskModels);
    containers.remove(containers.iterator().next());
  }

  @Test
  public void testGroupHappyPath() {
    Set<TaskModel> taskModels = generateTaskModels(5);

    Set<ContainerModel> containers = new GroupByContainerCount(2).group(taskModels);

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
  public void testGroupManyTasks() {
    Set<TaskModel> taskModels = generateTaskModels(21);

    Set<ContainerModel> containers = new GroupByContainerCount(2).group(taskModels);

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

  /**
   * Before:
   *  C0  C1
   * --------
   *  T0  T1
   *  T2  T3
   *  T4  T5
   *  T6  T7
   *  T8
   *
   * After:
   *  C0  C1  C2  C3
   * ----------------
   *  T0  T1  T6  T5
   *  T2  T3  T8  T7
   *  T4
   *
   *  NOTE for host affinity, it would help to have some additional logic to reassign tasks
   *  from C0 and C1 to containers that were on the same respective hosts, it wasn't implemented
   *  because the scenario is infrequent, the benefits are not guaranteed, and the code complexity
   *  wasn't worth it. It certainly could be implemented in the future.
   */
  @Test
  public void testBalancerAfterContainerIncrease() {
    Set<TaskModel> taskModels = generateTaskModels(9);
    Set<ContainerModel> prevContainers = new GroupByContainerCount(2).group(taskModels);
    Map<TaskName, String> prevTaskToContainerMapping = generateTaskContainerMapping(prevContainers);
    MetadataProviderImpl grouperContext = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), prevTaskToContainerMapping);

    Set<ContainerModel> containers = new GroupByContainerCount(4).group(taskModels, grouperContext);

    Map<String, ContainerModel> containersMap = new HashMap<>();
    for (ContainerModel container : containers) {
      containersMap.put(container.getId(), container);
    }

    assertEquals(4, containers.size());
    ContainerModel container0 = containersMap.get("0");
    ContainerModel container1 = containersMap.get("1");
    ContainerModel container2 = containersMap.get("2");
    ContainerModel container3 = containersMap.get("3");
    assertNotNull(container0);
    assertNotNull(container1);
    assertNotNull(container2);
    assertNotNull(container3);
    assertEquals("0", container0.getId());
    assertEquals("1", container1.getId());
    assertEquals(3, container0.getTasks().size());
    assertEquals(2, container1.getTasks().size());
    assertEquals(2, container2.getTasks().size());
    assertEquals(2, container3.getTasks().size());

    // Tasks 0-4 should stay on the same original containers
    assertTrue(container0.getTasks().containsKey(getTaskName(0)));
    assertTrue(container0.getTasks().containsKey(getTaskName(2)));
    assertTrue(container0.getTasks().containsKey(getTaskName(4)));
    assertTrue(container1.getTasks().containsKey(getTaskName(1)));
    assertTrue(container1.getTasks().containsKey(getTaskName(3)));
    // Tasks 5-8 should be reassigned to the new containers.
    // Consistency is the goal with these reassignments
    assertTrue(container2.getTasks().containsKey(getTaskName(8)));
    assertTrue(container2.getTasks().containsKey(getTaskName(6)));
    assertTrue(container3.getTasks().containsKey(getTaskName(5)));
    assertTrue(container3.getTasks().containsKey(getTaskName(7)));
  }

  /**
   * Before:
   *  C0  C1  C2  C3
   * ----------------
   *  T0  T1  T2  T3
   *  T4  T5  T6  T7
   *  T8
   *
   * After:
   *  C0  C1
   * --------
   *  T0  T1
   *  T4  T5
   *  T8  T7
   *  T6  T3
   *  T2
   *
   *  NOTE for host affinity, it would help to have some additional logic to reassign tasks
   *  from C2 and C3 to containers that were on the same respective hosts, it wasn't implemented
   *  because the scenario is infrequent, the benefits are not guaranteed, and the code complexity
   *  wasn't worth it. It certainly could be implemented in the future.
   */
  @Test
  public void testBalancerAfterContainerDecrease() {
    Set<TaskModel> taskModels = generateTaskModels(9);
    Set<ContainerModel> prevContainers = new GroupByContainerCount(4).group(taskModels);
    Map<TaskName, String> prevTaskToContainerMapping = generateTaskContainerMapping(prevContainers);
    MetadataProviderImpl grouperContext = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), prevTaskToContainerMapping);

    Set<ContainerModel> containers = new GroupByContainerCount(2).group(taskModels, grouperContext);

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
    assertEquals(5, container0.getTasks().size());
    assertEquals(4, container1.getTasks().size());

    // Tasks 0,4,8 and 1,5 should stay on the same original containers
    assertTrue(container0.getTasks().containsKey(getTaskName(0)));
    assertTrue(container0.getTasks().containsKey(getTaskName(4)));
    assertTrue(container0.getTasks().containsKey(getTaskName(8)));
    assertTrue(container1.getTasks().containsKey(getTaskName(1)));
    assertTrue(container1.getTasks().containsKey(getTaskName(5)));

    // Tasks 2,6 and 3,7 should be reassigned to the new containers.
    // Consistency is the goal with these reassignments
    assertTrue(container0.getTasks().containsKey(getTaskName(6)));
    assertTrue(container0.getTasks().containsKey(getTaskName(2)));
    assertTrue(container1.getTasks().containsKey(getTaskName(7)));
    assertTrue(container1.getTasks().containsKey(getTaskName(3)));
  }

  /**
   * Before:
   *  C0  C1  C2  C3
   * ----------------
   *  T0  T1  T2  T3
   *  T4  T5  T6  T7
   *  T8
   *
   * Intermediate:
   *  C0  C1
   * --------
   *  T0  T1
   *  T4  T5
   *  T8  T7
   *  T6  T3
   *  T2
   *
   *  After:
   *  C0  C1  C2
   * ------------
   *  T0  T1  T6
   *  T4  T5  T2
   *  T8  T7  T3
   */
  @Test
  public void testBalancerMultipleReblances() {
    // Before
    Set<TaskModel> taskModels = generateTaskModels(9);
    Set<ContainerModel> prevContainers = new GroupByContainerCount(4).group(taskModels);
    Map<TaskName, String> prevTaskToContainerMapping = generateTaskContainerMapping(prevContainers);
    MetadataProviderImpl grouperContext = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), prevTaskToContainerMapping);

    // First balance
    Set<ContainerModel> containers = new GroupByContainerCount(2).group(taskModels, grouperContext);

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
    assertEquals(5, container0.getTasks().size());
    assertEquals(4, container1.getTasks().size());

    // Tasks 0,4,8 and 1,5 should stay on the same original containers
    assertTrue(container0.getTasks().containsKey(getTaskName(0)));
    assertTrue(container0.getTasks().containsKey(getTaskName(4)));
    assertTrue(container0.getTasks().containsKey(getTaskName(8)));
    assertTrue(container1.getTasks().containsKey(getTaskName(1)));
    assertTrue(container1.getTasks().containsKey(getTaskName(5)));

    // Tasks 2,6 and 3,7 should be reassigned to the new containers.
    // Consistency is the goal with these reassignments
    assertTrue(container0.getTasks().containsKey(getTaskName(6)));
    assertTrue(container0.getTasks().containsKey(getTaskName(2)));
    assertTrue(container1.getTasks().containsKey(getTaskName(7)));
    assertTrue(container1.getTasks().containsKey(getTaskName(3)));

    // Second balance
    prevTaskToContainerMapping = generateTaskContainerMapping(prevContainers);

    MetadataProviderImpl grouperContext1 = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), prevTaskToContainerMapping);
    containers = new GroupByContainerCount(3).group(taskModels, grouperContext1);

    containersMap = new HashMap<>();
    for (ContainerModel container : containers) {
      containersMap.put(container.getId(), container);
    }

    assertEquals(3, containers.size());
    container0 = containersMap.get("0");
    container1 = containersMap.get("1");
    ContainerModel container2 = containersMap.get("2");
    assertNotNull(container0);
    assertNotNull(container1);
    assertNotNull(container2);
    assertEquals("0", container0.getId());
    assertEquals("1", container1.getId());
    assertEquals("2", container2.getId());
    assertEquals(3, container0.getTasks().size());
    assertEquals(3, container1.getTasks().size());
    assertEquals(3, container2.getTasks().size());

    // Tasks 0,4,8 and 1,5,7 should stay on the same original containers
    assertTrue(container0.getTasks().containsKey(getTaskName(0)));
    assertTrue(container0.getTasks().containsKey(getTaskName(4)));
    assertTrue(container0.getTasks().containsKey(getTaskName(8)));
    assertTrue(container1.getTasks().containsKey(getTaskName(1)));
    assertTrue(container1.getTasks().containsKey(getTaskName(5)));
    assertTrue(container1.getTasks().containsKey(getTaskName(7)));

    // Tasks 2,6 and 3 should be reassigned to the new container.
    // Consistency is the goal with these reassignments
    assertTrue(container2.getTasks().containsKey(getTaskName(6)));
    assertTrue(container2.getTasks().containsKey(getTaskName(2)));
    assertTrue(container2.getTasks().containsKey(getTaskName(3)));
  }

  /**
   * Before:
   *  C0  C1
   * --------
   *  T0  T1
   *  T2  T3
   *  T4  T5
   *  T6  T7
   *  T8
   *
   *  After:
   *  C0  C1
   * --------
   *  T0  T1
   *  T2  T3
   *  T4  T5
   *  T6  T7
   *  T8
   */
  @Test
  public void testBalancerAfterContainerSame() {
    Set<TaskModel> taskModels = generateTaskModels(9);
    Set<ContainerModel> prevContainers = new GroupByContainerCount(2).group(taskModels);
    Map<TaskName, String> prevTaskToContainerMapping = generateTaskContainerMapping(prevContainers);

    MetadataProviderImpl grouperContext = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), prevTaskToContainerMapping);
    Set<ContainerModel> containers = new GroupByContainerCount(2).group(taskModels, grouperContext);

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
    assertEquals(5, container0.getTasks().size());
    assertEquals(4, container1.getTasks().size());

    assertTrue(container0.getTasks().containsKey(getTaskName(0)));
    assertTrue(container0.getTasks().containsKey(getTaskName(2)));
    assertTrue(container0.getTasks().containsKey(getTaskName(4)));
    assertTrue(container0.getTasks().containsKey(getTaskName(6)));
    assertTrue(container0.getTasks().containsKey(getTaskName(8)));
    assertTrue(container1.getTasks().containsKey(getTaskName(1)));
    assertTrue(container1.getTasks().containsKey(getTaskName(3)));
    assertTrue(container1.getTasks().containsKey(getTaskName(5)));
    assertTrue(container1.getTasks().containsKey(getTaskName(7)));
  }

  /**
   * Verifies the ability to have a custom task-container mapping that is *deliberately* unbalanced.
   *
   * Before:
   *  C0  C1
   * --------
   *  T0  T6
   *  T1  T7
   *  T2  T8
   *  T3
   *  T4
   *  T5
   *
   *  After:
   *  C0  C1
   * --------
   *  T0  T6
   *  T1  T7
   *  T2  T8
   *  T3
   *  T4
   *  T5
   */
  @Test
  public void testBalancerAfterContainerSameCustomAssignment() {
    Set<TaskModel> taskModels = generateTaskModels(9);

    Map<TaskName, String> prevTaskToContainerMapping = new HashMap<>();
    prevTaskToContainerMapping.put(getTaskName(0), "0");
    prevTaskToContainerMapping.put(getTaskName(1), "0");
    prevTaskToContainerMapping.put(getTaskName(2), "0");
    prevTaskToContainerMapping.put(getTaskName(3), "0");
    prevTaskToContainerMapping.put(getTaskName(4), "0");
    prevTaskToContainerMapping.put(getTaskName(5), "0");
    prevTaskToContainerMapping.put(getTaskName(6), "1");
    prevTaskToContainerMapping.put(getTaskName(7), "1");
    prevTaskToContainerMapping.put(getTaskName(8), "1");

    MetadataProviderImpl grouperContext = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), prevTaskToContainerMapping);
    Set<ContainerModel> containers = new GroupByContainerCount(2).group(taskModels, grouperContext);

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
    assertEquals(6, container0.getTasks().size());
    assertEquals(3, container1.getTasks().size());

    assertTrue(container0.getTasks().containsKey(getTaskName(0)));
    assertTrue(container0.getTasks().containsKey(getTaskName(1)));
    assertTrue(container0.getTasks().containsKey(getTaskName(2)));
    assertTrue(container0.getTasks().containsKey(getTaskName(3)));
    assertTrue(container0.getTasks().containsKey(getTaskName(4)));
    assertTrue(container0.getTasks().containsKey(getTaskName(5)));
    assertTrue(container1.getTasks().containsKey(getTaskName(6)));
    assertTrue(container1.getTasks().containsKey(getTaskName(7)));
    assertTrue(container1.getTasks().containsKey(getTaskName(8)));
  }

  /**
   * Verifies the ability to have a custom task-container mapping that is *deliberately* unbalanced.
   *
   * Before:
   *  C0  C1
   * --------
   *  T0  T1
   *      T2
   *      T3
   *      T4
   *      T5
   *
   *  After:
   *  C0  C1  C2
   * ------------
   *  T0  T1  T4
   *  T5  T2  T3
   *
   *  The key here is that C0, which is not one of the new containers was under-allocated.
   *  This is an important case because this scenario, while impossible with GroupByContainerCount.group()
   *  could occur when the grouper class is switched or if there is a custom mapping.
   */
  @Test
  public void testBalancerAfterContainerSameCustomAssignmentAndContainerIncrease() {
    Set<TaskModel> taskModels = generateTaskModels(6);

    Map<TaskName, String> prevTaskToContainerMapping = new HashMap<>();
    prevTaskToContainerMapping.put(getTaskName(0), "0");
    prevTaskToContainerMapping.put(getTaskName(1), "1");
    prevTaskToContainerMapping.put(getTaskName(2), "1");
    prevTaskToContainerMapping.put(getTaskName(3), "1");
    prevTaskToContainerMapping.put(getTaskName(4), "1");
    prevTaskToContainerMapping.put(getTaskName(5), "1");
    MetadataProviderImpl grouperContext = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), prevTaskToContainerMapping);

    Set<ContainerModel> containers = new GroupByContainerCount(3).group(taskModels, grouperContext);

    Map<String, ContainerModel> containersMap = new HashMap<>();
    for (ContainerModel container : containers) {
      containersMap.put(container.getId(), container);
    }

    assertEquals(3, containers.size());
    ContainerModel container0 = containersMap.get("0");
    ContainerModel container1 = containersMap.get("1");
    ContainerModel container2 = containersMap.get("2");
    assertNotNull(container0);
    assertNotNull(container1);
    assertNotNull(container2);
    assertEquals("0", container0.getId());
    assertEquals("1", container1.getId());
    assertEquals("2", container2.getId());
    assertEquals(2, container0.getTasks().size());
    assertEquals(2, container1.getTasks().size());
    assertEquals(2, container1.getTasks().size());

    assertTrue(container0.getTasks().containsKey(getTaskName(0)));
    assertTrue(container0.getTasks().containsKey(getTaskName(5)));
    assertTrue(container1.getTasks().containsKey(getTaskName(1)));
    assertTrue(container1.getTasks().containsKey(getTaskName(2)));
    assertTrue(container2.getTasks().containsKey(getTaskName(4)));
    assertTrue(container2.getTasks().containsKey(getTaskName(3)));
  }

  @Test
  public void testBalancerOldContainerCountOne() {
    Set<TaskModel> taskModels = generateTaskModels(3);
    Set<ContainerModel> prevContainers = new GroupByContainerCount(1).group(taskModels);
    Map<TaskName, String> prevTaskToContainerMapping = generateTaskContainerMapping(prevContainers);
    MetadataProviderImpl grouperContext = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), prevTaskToContainerMapping);

    Set<ContainerModel> groupContainers = new GroupByContainerCount(3).group(taskModels);
    Set<ContainerModel> balanceContainers = new GroupByContainerCount(3).group(taskModels, grouperContext);

    // Results should be the same as calling group()
    assertEquals(groupContainers, balanceContainers);
  }

  @Test
  public void testBalancerNewContainerCountOne() {
    Set<TaskModel> taskModels = generateTaskModels(3);
    Set<ContainerModel> prevContainers = new GroupByContainerCount(3).group(taskModels);
    Map<TaskName, String> prevTaskToContainerMapping = generateTaskContainerMapping(prevContainers);
    MetadataProviderImpl grouperContext = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), prevTaskToContainerMapping);

    Set<ContainerModel> groupContainers = new GroupByContainerCount(1).group(taskModels);
    Set<ContainerModel> balanceContainers = new GroupByContainerCount(1).group(taskModels, grouperContext);

    // Results should be the same as calling group()
    assertEquals(groupContainers, balanceContainers);
  }

  @Test
  public void testBalancerEmptyTaskMapping() {
    Set<TaskModel> taskModels = generateTaskModels(3);
    MetadataProviderImpl grouperContext = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>());

    Set<ContainerModel> groupContainers = new GroupByContainerCount(1).group(taskModels);
    Set<ContainerModel> balanceContainers = new GroupByContainerCount(1).group(taskModels, grouperContext);

    // Results should be the same as calling group()
    assertEquals(groupContainers, balanceContainers);
  }

  @Test
  public void testGroupTaskCountIncrease() {
    int taskCount = 3;
    Set<TaskModel> taskModels = generateTaskModels(taskCount);
    Set<ContainerModel> prevContainers = new GroupByContainerCount(2).group(generateTaskModels(taskCount - 1)); // Here's the key step
    Map<TaskName, String> prevTaskToContainerMapping = generateTaskContainerMapping(prevContainers);
    MetadataProviderImpl grouperContext = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), prevTaskToContainerMapping);

    Set<ContainerModel> groupContainers = new GroupByContainerCount(1).group(taskModels);
    Set<ContainerModel> balanceContainers = new GroupByContainerCount(1).group(taskModels, grouperContext);

    // Results should be the same as calling group()
    assertEquals(groupContainers, balanceContainers);
  }

  @Test
  public void testGroupTaskCountDecrease() {
    int taskCount = 3;
    Set<TaskModel> taskModels = generateTaskModels(taskCount);
    Set<ContainerModel> prevContainers = new GroupByContainerCount(3).group(generateTaskModels(taskCount + 1)); // Here's the key step
    Map<TaskName, String> prevTaskToContainerMapping = generateTaskContainerMapping(prevContainers);
    MetadataProviderImpl grouperContext = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), prevTaskToContainerMapping);

    Set<ContainerModel> groupContainers = new GroupByContainerCount(1).group(taskModels);
    Set<ContainerModel> balanceContainers = new GroupByContainerCount(1).group(taskModels, grouperContext);

    // Results should be the same as calling group()
    assertEquals(groupContainers, balanceContainers);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBalancerNewContainerCountGreaterThanTasks() {
    Set<TaskModel> taskModels = generateTaskModels(3);
    Set<ContainerModel> prevContainers = new GroupByContainerCount(3).group(taskModels);
    Map<TaskName, String> prevTaskToContainerMapping = generateTaskContainerMapping(prevContainers);
    MetadataProviderImpl grouperContext = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), prevTaskToContainerMapping);

    new GroupByContainerCount(5).group(taskModels, grouperContext);     // Should throw
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBalancerEmptyTasks() {
    Set<TaskModel> taskModels = generateTaskModels(3);
    Set<ContainerModel> prevContainers = new GroupByContainerCount(3).group(taskModels);
    Map<TaskName, String> prevTaskToContainerMapping = generateTaskContainerMapping(prevContainers);
    MetadataProviderImpl grouperContext = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), prevTaskToContainerMapping);

    new GroupByContainerCount(5).group(new HashSet<>(), grouperContext);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testBalancerResultImmutable() {
    Set<TaskModel> taskModels = generateTaskModels(3);
    Set<ContainerModel> prevContainers = new GroupByContainerCount(3).group(taskModels);
    Map<TaskName, String> prevTaskToContainerMapping = generateTaskContainerMapping(prevContainers);
    MetadataProviderImpl grouperContext = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), prevTaskToContainerMapping);

    Set<ContainerModel> containers = new GroupByContainerCount(2).group(taskModels, grouperContext);
    containers.remove(containers.iterator().next());
  }

  @Test(expected = SamzaException.class)
  public void testBalancerThrowsOnNonIntegerContainerIds() {
    Set<TaskModel> taskModels = generateTaskModels(3);
    Set<ContainerModel> prevContainers = new HashSet<>();
    taskModels.forEach(model -> prevContainers.add(new ContainerModel(UUID.randomUUID().toString(), Collections.singletonMap(model.getTaskName(), model))));
    Map<TaskName, String> prevTaskToContainerMapping = generateTaskContainerMapping(prevContainers);
    MetadataProviderImpl grouperContext = new MetadataProviderImpl(new HashMap<>(), new HashMap<>(), new HashMap<>(), prevTaskToContainerMapping);
    new GroupByContainerCount(3).group(taskModels, grouperContext); //Should throw
  }

  @Test
  public void testBalancerWithNullLocalityManager() {
    Set<TaskModel> taskModels = generateTaskModels(3);

    Set<ContainerModel> groupContainers = new GroupByContainerCount(3).group(taskModels);
    Set<ContainerModel> balanceContainers = new GroupByContainerCount(3).balance(taskModels, null);

    // Results should be the same as calling group()
    assertEquals(groupContainers, balanceContainers);
  }
}
