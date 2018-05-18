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

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.stream.CoordinatorStreamManager;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory.MockCoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory.MockCoordinatorStreamSystemProducer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTaskAssignmentManager {
  private final Config config = new MapConfig(
      new HashMap<String, String>() {
        {
          this.put("job.name", "test-job");
          this.put("job.coordinator.system", "test-kafka");
        }
      });

  @Before
  public void setup() {
    MockCoordinatorStreamSystemFactory.enableMockConsumerCache();
  }

  @After
  public void tearDown() {
    MockCoordinatorStreamSystemFactory.disableMockConsumerCache();
  }

  @Test public void testTaskAssignmentManager() throws Exception {
    MockCoordinatorStreamSystemFactory mockCoordinatorStreamSystemFactory =
        new MockCoordinatorStreamSystemFactory();
    MockCoordinatorStreamSystemProducer producer =
        mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemProducer(config, null);
    MockCoordinatorStreamSystemConsumer consumer =
        mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(config, null);
    consumer.register();
    CoordinatorStreamManager
        coordinatorStreamManager = new CoordinatorStreamManager(producer, consumer);
    TaskAssignmentManager taskAssignmentManager = new TaskAssignmentManager(coordinatorStreamManager);

    assertTrue(producer.isRegistered());
    assertEquals(producer.getRegisteredSource(), "SamzaTaskAssignmentManager");

    coordinatorStreamManager.start();
    assertTrue(producer.isStarted());
    assertTrue(consumer.isStarted());

    Map<String, String> expectedMap =
      new HashMap<String, String>() {
        {
          this.put("Task0", "0");
          this.put("Task1", "1");
          this.put("Task2", "2");
          this.put("Task3", "0");
          this.put("Task4", "1");
        }
      };

    for (Map.Entry<String, String> entry : expectedMap.entrySet()) {
      taskAssignmentManager.writeTaskContainerMapping(entry.getKey(), entry.getValue());
    }

    Map<String, String> localMap = taskAssignmentManager.readTaskAssignment();

    assertEquals(expectedMap, localMap);

    coordinatorStreamManager.stop();
    assertTrue(producer.isStopped());
    assertTrue(consumer.isStopped());
  }

  @Test public void testDeleteMappings() throws Exception {
    MockCoordinatorStreamSystemFactory mockCoordinatorStreamSystemFactory =
        new MockCoordinatorStreamSystemFactory();
    MockCoordinatorStreamSystemProducer producer =
        mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemProducer(config, null);
    MockCoordinatorStreamSystemConsumer consumer =
        mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(config, null);
    consumer.register();
    CoordinatorStreamManager
        coordinatorStreamManager = new CoordinatorStreamManager(producer, consumer);
    TaskAssignmentManager taskAssignmentManager = new TaskAssignmentManager(coordinatorStreamManager);

    assertTrue(producer.isRegistered());
    assertEquals(producer.getRegisteredSource(), "SamzaTaskAssignmentManager");

    coordinatorStreamManager.start();
    assertTrue(producer.isStarted());
    assertTrue(consumer.isStarted());

    Map<String, String> expectedMap =
      new HashMap<String, String>() {
        {
          this.put("Task0", "0");
          this.put("Task1", "1");
        }
      };

    for (Map.Entry<String, String> entry : expectedMap.entrySet()) {
      taskAssignmentManager.writeTaskContainerMapping(entry.getKey(), entry.getValue());
    }

    Map<String, String> localMap = taskAssignmentManager.readTaskAssignment();
    assertEquals(expectedMap, localMap);

    taskAssignmentManager.deleteTaskContainerMappings(localMap.keySet());
    Map<String, String> deletedMap = taskAssignmentManager.readTaskAssignment();
    assertTrue(deletedMap.isEmpty());

    coordinatorStreamManager.stop();
    assertTrue(producer.isStopped());
    assertTrue(consumer.isStopped());
  }

  @Test public void testTaskAssignmentManagerEmptyCoordinatorStream() throws Exception {
    MockCoordinatorStreamSystemFactory mockCoordinatorStreamSystemFactory =
        new MockCoordinatorStreamSystemFactory();
    MockCoordinatorStreamSystemProducer producer =
        mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemProducer(config, null);
    MockCoordinatorStreamSystemConsumer consumer =
        mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(config, null);
    consumer.register();
    CoordinatorStreamManager
        coordinatorStreamManager = new CoordinatorStreamManager(producer, consumer);
    TaskAssignmentManager taskAssignmentManager = new TaskAssignmentManager(coordinatorStreamManager);

    assertTrue(producer.isRegistered());
    assertEquals(producer.getRegisteredSource(), "SamzaTaskAssignmentManager");

    coordinatorStreamManager.start();
    assertTrue(producer.isStarted());
    assertTrue(consumer.isStarted());

    Map<String, String> expectedMap = new HashMap<>();
    Map<String, String> localMap = taskAssignmentManager.readTaskAssignment();

    assertEquals(expectedMap, localMap);

    coordinatorStreamManager.stop();
    assertTrue(producer.isStopped());
    assertTrue(consumer.isStopped());
  }
}
