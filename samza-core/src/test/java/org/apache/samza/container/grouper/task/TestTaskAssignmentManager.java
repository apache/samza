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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStoreTestUtil;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory;
import org.apache.samza.coordinator.stream.messages.SetTaskContainerMapping;
import org.apache.samza.coordinator.stream.messages.SetTaskModeMapping;
import org.apache.samza.job.model.TaskMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    Map<String, String> expectedMap = ImmutableMap.of("Task0", "0", "Task1", "1", "Task2", "2", "Task3", "0", "Task4", "1");
    Map<String, Map<String, TaskMode>> taskContainerMappings = ImmutableMap.of(
        "0", ImmutableMap.of("Task0", TaskMode.Active, "Task3", TaskMode.Active),
        "1", ImmutableMap.of("Task1", TaskMode.Active, "Task4", TaskMode.Active),
        "2", ImmutableMap.of("Task2", TaskMode.Active)
    );

    taskAssignmentManager.writeTaskContainerMappings(taskContainerMappings);

    Map<String, String> localMap = taskAssignmentManager.readTaskAssignment();

    assertEquals(expectedMap, localMap);

    taskAssignmentManager.close();
  }

  @Test
  public void testDeleteMappings() {
    Map<String, String> expectedMap = ImmutableMap.of("Task0", "0", "Task1", "1");
    Map<String, Map<String, TaskMode>> taskContainerMappings = ImmutableMap.of(
        "0", ImmutableMap.of("Task0", TaskMode.Active),
        "1", ImmutableMap.of("Task1", TaskMode.Active)
    );

    taskAssignmentManager.writeTaskContainerMappings(taskContainerMappings);

    Map<String, String> localMap = taskAssignmentManager.readTaskAssignment();
    assertEquals(expectedMap, localMap);

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
