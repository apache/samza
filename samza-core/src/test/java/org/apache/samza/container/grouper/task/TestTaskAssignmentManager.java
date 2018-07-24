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
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.*;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(CoordinatorStreamUtil.class)
public class TestTaskAssignmentManager {

  private MockCoordinatorStreamSystemFactory mockCoordinatorStreamSystemFactory;

  private final Config config = new MapConfig(ImmutableMap.of("job.name", "test-job", "job.coordinator.system", "test-kafka"));

  @Before
  public void setup() {
    MockCoordinatorStreamSystemFactory.enableMockConsumerCache();
    mockCoordinatorStreamSystemFactory = new MockCoordinatorStreamSystemFactory();
    PowerMockito.mockStatic(CoordinatorStreamUtil.class);
    when(CoordinatorStreamUtil.getCoordinatorSystemFactory(anyObject())).thenReturn(mockCoordinatorStreamSystemFactory);
    when(CoordinatorStreamUtil.getCoordinatorSystemStream(anyObject())).thenReturn(new SystemStream("test-kafka", "test"));
    when(CoordinatorStreamUtil.getCoordinatorStreamName(anyObject(), anyObject())).thenReturn("test");
  }

  @After
  public void tearDown() {
    MockCoordinatorStreamSystemFactory.disableMockConsumerCache();
  }

  @Test
  public void testTaskAssignmentManager() {
    TaskAssignmentManager taskAssignmentManager = new TaskAssignmentManager(config, new MetricsRegistryMap());
    taskAssignmentManager.init(new SamzaContainerContext("1", config, new ArrayList<>(), new MetricsRegistryMap()));

    Map<String, String> expectedMap = ImmutableMap.of("Task0", "0", "Task1", "1", "Task2", "2", "Task3", "0", "Task4", "1");

    for (Map.Entry<String, String> entry : expectedMap.entrySet()) {
      taskAssignmentManager.writeTaskContainerMapping(entry.getKey(), entry.getValue());
    }

    Map<String, String> localMap = taskAssignmentManager.readTaskAssignment();

    assertEquals(expectedMap, localMap);

    taskAssignmentManager.close();
  }

  @Test public void testDeleteMappings() {
    TaskAssignmentManager taskAssignmentManager = new TaskAssignmentManager(config, new MetricsRegistryMap());
    taskAssignmentManager.init(new SamzaContainerContext("1", config, new ArrayList<>(), new MetricsRegistryMap()));

    Map<String, String> expectedMap = ImmutableMap.of("Task0", "0", "Task1", "1");

    for (Map.Entry<String, String> entry : expectedMap.entrySet()) {
      taskAssignmentManager.writeTaskContainerMapping(entry.getKey(), entry.getValue());
    }

    Map<String, String> localMap = taskAssignmentManager.readTaskAssignment();
    assertEquals(expectedMap, localMap);

    taskAssignmentManager.deleteTaskContainerMappings(localMap.keySet());

    Map<String, String> deletedMap = taskAssignmentManager.readTaskAssignment();
    assertTrue(deletedMap.isEmpty());

    taskAssignmentManager.close();
  }

  @Test public void testTaskAssignmentManagerEmptyCoordinatorStream() {
    TaskAssignmentManager taskAssignmentManager = new TaskAssignmentManager(config, new MetricsRegistryMap());
    taskAssignmentManager.init(new SamzaContainerContext("1", config, new ArrayList<>(), new MetricsRegistryMap()));

    Map<String, String> expectedMap = new HashMap<>();
    Map<String, String> localMap = taskAssignmentManager.readTaskAssignment();

    assertEquals(expectedMap, localMap);

    taskAssignmentManager.close();
  }
}
