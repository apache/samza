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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory.MockCoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory.MockCoordinatorStreamSystemProducer;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link LocalityManager}
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest(CoordinatorStreamUtil.class)
public class TestLocalityManager {

  private MockCoordinatorStreamSystemFactory mockCoordinatorStreamSystemFactory;
  private final Config config = new MapConfig(ImmutableMap.of("job.name", "test-job", "job.coordinator.system", "test-kafka"));

  @Before
  public void setup() {
    mockCoordinatorStreamSystemFactory = new MockCoordinatorStreamSystemFactory();
    MockCoordinatorStreamSystemFactory.enableMockConsumerCache();
    PowerMockito.mockStatic(CoordinatorStreamUtil.class);
    when(CoordinatorStreamUtil.getCoordinatorSystemFactory(anyObject())).thenReturn(mockCoordinatorStreamSystemFactory);
    when(CoordinatorStreamUtil.getCoordinatorSystemStream(anyObject())).thenReturn(new SystemStream("test-kafka", "test"));
    when(CoordinatorStreamUtil.getCoordinatorStreamName(anyObject(), anyObject())).thenReturn("test");
  }

  @After
  public void tearDown() {
    MockCoordinatorStreamSystemFactory.disableMockConsumerCache();
  }

  @Test public void testLocalityManager() {
    LocalityManager localityManager = new LocalityManager(config, new MetricsRegistryMap());

    localityManager.init(new SamzaContainerContext("0", config, new ArrayList<>(), new MetricsRegistryMap()));
    localityManager.writeContainerToHostMapping("0", "localhost", "jmx:localhost:8080", "jmx:tunnel:localhost:9090");
    Map<String, Map<String, String>> localMap = localityManager.readContainerLocality();
    Map<String, Map<String, String>> expectedMap =
      new HashMap<String, Map<String, String>>() {
        {
          this.put("0",
            new HashMap<String, String>() {
              {
                this.put(SetContainerHostMapping.HOST_KEY, "localhost");
                this.put(SetContainerHostMapping.JMX_URL_KEY, "");
                this.put(SetContainerHostMapping.JMX_TUNNELING_URL_KEY, "");
              }
            });
        }
      };
    assertEquals(expectedMap, localMap);

    localityManager.close();

    MockCoordinatorStreamSystemProducer producer = mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemProducer(config, null);
    MockCoordinatorStreamSystemConsumer consumer = mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(config, null);
    assertTrue(producer.isStopped());
    assertTrue(consumer.isStopped());
  }

  @Test public void testWriteOnlyLocalityManager() {
    LocalityManager localityManager = new LocalityManager(config, new MetricsRegistryMap());

    localityManager.writeContainerToHostMapping("1", "localhost", "jmx:localhost:8181", "jmx:tunnel:localhost:9191");

    localityManager.init(new SamzaContainerContext("0", config, new ArrayList<>(), new MetricsRegistryMap()));

    assertEquals(localityManager.readContainerLocality().size(), 1);

    SetContainerHostMapping expectedContainerMap = new SetContainerHostMapping("SamzaContainer-1", "1", "localhost", "", "");
    assertEquals(ImmutableMap.of("1", expectedContainerMap.getMessageMap().get("values")), localityManager.readContainerLocality());

    localityManager.close();

    MockCoordinatorStreamSystemProducer producer = mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemProducer(config, null);
    MockCoordinatorStreamSystemConsumer consumer = mockCoordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(config, null);
    assertTrue(producer.isStopped());
    assertTrue(consumer.isStopped());
  }
}
