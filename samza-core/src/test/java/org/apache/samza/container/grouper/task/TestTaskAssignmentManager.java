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
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory.MockCoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory.MockCoordinatorStreamSystemProducer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTaskAssignmentManager {

  private final MockCoordinatorStreamSystemFactory mockCoordinatorStreamSystemFactory =
      new MockCoordinatorStreamSystemFactory();
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
    MockCoordinatorStreamSystemProducer producer =
        mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemProducer(config, null);
    MockCoordinatorStreamSystemConsumer consumer =
        mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(config, null);
    consumer.register();
    TaskAssignmentManager taskAssignmentManager = new TaskAssignmentManager(producer, consumer);

    assertTrue(producer.isRegistered());
    assertEquals(producer.getRegisteredSource(), "SamzaTaskAssignmentManager");

    taskAssignmentManager.start();
    assertTrue(producer.isStarted());
    assertTrue(consumer.isStarted());

    Map<String, Integer> expectedMap =
        new HashMap<String, Integer>() {
          {
            this.put("Task0", new Integer(0));
            this.put("Task1", new Integer(1));
            this.put("Task2", new Integer(2));
            this.put("Task3", new Integer(0));
            this.put("Task4", new Integer(1));
          }
        };

    for (Map.Entry<String, Integer> entry : expectedMap.entrySet()) {
      taskAssignmentManager.writeTaskContainerMapping(entry.getKey(), entry.getValue());
    }

    Map<String, Integer> localMap = taskAssignmentManager.readTaskAssignment();

    assertEquals(expectedMap, localMap);

    taskAssignmentManager.stop();
    assertTrue(producer.isStopped());
    assertTrue(consumer.isStopped());
  }

  @Test public void testDeleteMappings() throws Exception {
    MockCoordinatorStreamSystemProducer producer =
        mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemProducer(config, null);
    MockCoordinatorStreamSystemConsumer consumer =
        mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(config, null);
    consumer.register();
    TaskAssignmentManager taskAssignmentManager = new TaskAssignmentManager(producer, consumer);

    assertTrue(producer.isRegistered());
    assertEquals(producer.getRegisteredSource(), "SamzaTaskAssignmentManager");

    taskAssignmentManager.start();
    assertTrue(producer.isStarted());
    assertTrue(consumer.isStarted());

    Map<String, Integer> expectedMap =
        new HashMap<String, Integer>() {
          {
            this.put("Task0", new Integer(0));
            this.put("Task1", new Integer(1));
          }
        };

    for (Map.Entry<String, Integer> entry : expectedMap.entrySet()) {
      taskAssignmentManager.writeTaskContainerMapping(entry.getKey(), entry.getValue());
    }

    Map<String, Integer> localMap = taskAssignmentManager.readTaskAssignment();
    assertEquals(expectedMap, localMap);

    taskAssignmentManager.deleteTaskContainerMappings(localMap.keySet());
    Map<String, Integer> deletedMap = taskAssignmentManager.readTaskAssignment();
    assertTrue(deletedMap.isEmpty());

    taskAssignmentManager.stop();
    assertTrue(producer.isStopped());
    assertTrue(consumer.isStopped());
  }

  @Test public void testTaskAssignmentManagerEmptyCoordinatorStream() throws Exception {
    MockCoordinatorStreamSystemProducer producer =
        mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemProducer(config, null);
    MockCoordinatorStreamSystemConsumer consumer =
        mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(config, null);
    consumer.register();
    TaskAssignmentManager taskAssignmentManager = new TaskAssignmentManager(producer, consumer);

    assertTrue(producer.isRegistered());
    assertEquals(producer.getRegisteredSource(), "SamzaTaskAssignmentManager");

    taskAssignmentManager.start();
    assertTrue(producer.isStarted());
    assertTrue(consumer.isStarted());

    Map<String, Integer> expectedMap = new HashMap<>();
    Map<String, Integer> localMap = taskAssignmentManager.readTaskAssignment();

    assertEquals(expectedMap, localMap);

    taskAssignmentManager.stop();
    assertTrue(producer.isStopped());
    assertTrue(consumer.isStopped());
  }
}
