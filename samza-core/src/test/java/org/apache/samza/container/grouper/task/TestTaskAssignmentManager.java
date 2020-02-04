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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStoreTestUtil;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory;
import org.apache.samza.coordinator.stream.messages.SetTaskContainerMapping;
import org.apache.samza.coordinator.stream.messages.SetTaskModeMapping;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTaskAssignmentManager {

  private static final Config CONFIG = new MapConfig(ImmutableMap.of("job.name", "test-job", "job.coordinator.system", "test-kafka"));

  private CoordinatorStreamStore coordinatorStreamStore;
  private TaskAssignmentManager taskAssignmentManager;

  @Before
  public void setup() {
    CoordinatorStreamStoreTestUtil coordinatorStreamStoreTestUtil = new CoordinatorStreamStoreTestUtil(CONFIG);
    coordinatorStreamStore = coordinatorStreamStoreTestUtil.getCoordinatorStreamStore();
    taskAssignmentManager = new TaskAssignmentManager(new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, SetTaskContainerMapping.TYPE), new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, SetTaskModeMapping.TYPE));
  }

  @After
  public void tearDown() {
    MockCoordinatorStreamSystemFactory.disableMockConsumerCache();
  }

  @Test
  public void testTaskAssignmentManager() {
    String t0 = "Task0";
    String t1 = "Task1";
    String t2 = "Task2";
    String t3 = "Task3";
    String t4 = "Task4";

    ContainerModel c0 = createMockContainerModel("0", ImmutableList.of(t0, t3));
    ContainerModel c1 = createMockContainerModel("1", ImmutableList.of(t1, t4));
    ContainerModel c2 = createMockContainerModel("2", ImmutableList.of(t2));

    ImmutableMap<String, ContainerModel> expectedMap = ImmutableMap.of(t0, c0, t1, c1, t2, c2, t3, c0, t4, c1);

    taskAssignmentManager.writeTaskContainerAssignments(expectedMap);

    Map<String, String> localMap = taskAssignmentManager.readTaskAssignment();

    assertEquals(expectedMap.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().getId())), localMap);

    taskAssignmentManager.close();
  }

  public ContainerModel createMockContainerModel(String containerId, List<String> taskNames) {
    ContainerModel mockContainerModel = Mockito.mock(ContainerModel.class);
    HashMap<TaskName, TaskModel> taskModelMap = new HashMap<>();

    for (String taskName : taskNames) {
      TaskModel mockTaskModel = Mockito.mock(TaskModel.class);
      TaskName task = new TaskName(taskName);
      Mockito.doReturn(task).when(mockTaskModel).getTaskName();
      Mockito.doReturn(TaskMode.Active).when(mockTaskModel).getTaskMode();
      taskModelMap.put(task, mockTaskModel);
    }

    Mockito.doReturn(containerId).when(mockContainerModel).getId();
    Mockito.doReturn(taskModelMap).when(mockContainerModel).getTasks();
    return mockContainerModel;
  }

  @Test
  public void testDeleteMappings() {
    //Map<String, String> expectedMap = ImmutableMap.of("Task0", "0", "Task1", "1");
    String t0 = "Task0";
    String t1 = "Task1";

    ContainerModel c0 = createMockContainerModel("0", ImmutableList.of(t0));
    ContainerModel c1 = createMockContainerModel("1", ImmutableList.of(t1));

    ImmutableMap<String, ContainerModel> expectedMap = ImmutableMap.of(t0, c0, t1, c1);

    taskAssignmentManager.writeTaskContainerAssignments(expectedMap);

    Map<String, String> localMap = taskAssignmentManager.readTaskAssignment();
    assertEquals(expectedMap.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().getId())), localMap);

    taskAssignmentManager.deleteTaskContainerMappings(localMap.keySet());

    Map<String, String> deletedMap = taskAssignmentManager.readTaskAssignment();
    assertTrue(deletedMap.isEmpty());

    taskAssignmentManager.close();
  }

  @Test
  public void testTaskAssignmentManagerEmptyCoordinatorStream() {
    Map<String, String> expectedMap = new HashMap<>();
    Map<String, String> localMap = taskAssignmentManager.readTaskAssignment();

    assertEquals(expectedMap, localMap);

    taskAssignmentManager.close();
  }
}
