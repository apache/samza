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

package org.apache.samza.container;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory.MockCoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory.MockCoordinatorStreamSystemProducer;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link LocalityManager}
 */
public class TestLocalityManager {

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

  @Test public void testLocalityManager() throws Exception {
    MockCoordinatorStreamSystemProducer producer =
        mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemProducer(config, null);
    MockCoordinatorStreamSystemConsumer consumer =
        mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(config, null);
    LocalityManager localityManager = new LocalityManager(producer, consumer);

    try {
      localityManager.register(new TaskName("task-0"));
      fail("Should have thrown UnsupportedOperationException");
    } catch (UnsupportedOperationException uoe) {
      // expected
    }

    localityManager.register("containerId-0");
    assertTrue(producer.isRegistered());
    assertEquals(producer.getRegisteredSource(), "SamzaContainer-containerId-0");
    assertTrue(consumer.isRegistered());

    localityManager.start();
    assertTrue(producer.isStarted());
    assertTrue(consumer.isStarted());

    localityManager.writeContainerToHostMapping(0, "localhost", "jmx:localhost:8080", "jmx:tunnel:localhost:9090");
    Map<Integer, Map<String, String>> localMap = localityManager.readContainerLocality();
    Map<Integer, Map<String, String>> expectedMap =
        new HashMap<Integer, Map<String, String>>() {
          {
            this.put(new Integer(0),
                new HashMap<String, String>() {
                  {
                    this.put(SetContainerHostMapping.HOST_KEY, "localhost");
                    this.put(SetContainerHostMapping.JMX_URL_KEY, "jmx:localhost:8080");
                    this.put(SetContainerHostMapping.JMX_TUNNELING_URL_KEY, "jmx:tunnel:localhost:9090");
                  }
                });
          }
        };
    assertEquals(expectedMap, localMap);

    localityManager.stop();
    assertTrue(producer.isStopped());
    assertTrue(consumer.isStopped());
  }

  @Test public void testWriteOnlyLocalityManager() {
    MockCoordinatorStreamSystemProducer producer =
        mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemProducer(config, null);
    LocalityManager localityManager = new LocalityManager(producer);

    localityManager.register("containerId-1");
    assertTrue(producer.isRegistered());
    assertEquals(producer.getRegisteredSource(), "SamzaContainer-containerId-1");

    localityManager.start();
    assertTrue(producer.isStarted());

    localityManager.writeContainerToHostMapping(1, "localhost", "jmx:localhost:8181", "jmx:tunnel:localhost:9191");
    try {
      localityManager.readContainerLocality();
      fail("Should have thrown UnsupportedOperationException");
    } catch (UnsupportedOperationException uoe) {
      // expected
    }
    assertEquals(producer.getEnvelopes().size(), 1);
    CoordinatorStreamMessage coordinatorStreamMessage =
        MockCoordinatorStreamSystemFactory.deserializeCoordinatorStreamMessage(producer.getEnvelopes().get(0));

    SetContainerHostMapping expectedContainerMap =
        new SetContainerHostMapping("SamzaContainer-1", "1", "localhost", "jmx:localhost:8181",
            "jmx:tunnel:localhost:9191");
    assertEquals(expectedContainerMap, coordinatorStreamMessage);

    localityManager.stop();
    assertTrue(producer.isStopped());
  }
}
