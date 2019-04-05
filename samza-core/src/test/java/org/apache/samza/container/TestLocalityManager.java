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
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStoreTestUtil;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory.MockCoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory.MockCoordinatorStreamSystemProducer;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestLocalityManager {

  private static final Config CONFIG = new MapConfig(ImmutableMap.of("job.name", "test-job", "job.coordinator.system", "test-kafka"));

  private CoordinatorStreamStore coordinatorStreamStore;
  private CoordinatorStreamStoreTestUtil coordinatorStreamStoreTestUtil;

  @Before
  public void setup() {
    coordinatorStreamStoreTestUtil = new CoordinatorStreamStoreTestUtil(CONFIG);
    coordinatorStreamStore = coordinatorStreamStoreTestUtil.getCoordinatorStreamStore();
  }

  @After
  public void tearDown() {
    MockCoordinatorStreamSystemFactory.disableMockConsumerCache();
  }

  @Test
  public void testLocalityManager() {
    LocalityManager localityManager = new LocalityManager(new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, SetContainerHostMapping.TYPE));

    localityManager.writeContainerToHostMapping("0", "localhost");
    Map<String, Map<String, String>> localMap = localityManager.readContainerLocality();
    Map<String, Map<String, String>> expectedMap =
      new HashMap<String, Map<String, String>>() {
        {
          this.put("0",
            new HashMap<String, String>() {
              {
                this.put(SetContainerHostMapping.HOST_KEY, "localhost");
              }
            });
        }
      };
    assertEquals(expectedMap, localMap);

    localityManager.close();

    MockCoordinatorStreamSystemProducer producer = coordinatorStreamStoreTestUtil.getMockCoordinatorStreamSystemProducer();
    MockCoordinatorStreamSystemConsumer consumer = coordinatorStreamStoreTestUtil.getMockCoordinatorStreamSystemConsumer();
    assertTrue(producer.isStopped());
    assertTrue(consumer.isStopped());
  }

  @Test
  public void testWriteOnlyLocalityManager() {
    LocalityManager localityManager = new LocalityManager(new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, SetContainerHostMapping.TYPE));

    localityManager.writeContainerToHostMapping("1", "localhost");

    assertEquals(localityManager.readContainerLocality().size(), 1);

    assertEquals(ImmutableMap.of("1", ImmutableMap.of("host", "localhost")), localityManager.readContainerLocality());

    localityManager.close();

    MockCoordinatorStreamSystemProducer producer = coordinatorStreamStoreTestUtil.getMockCoordinatorStreamSystemProducer();
    MockCoordinatorStreamSystemConsumer consumer = coordinatorStreamStoreTestUtil.getMockCoordinatorStreamSystemConsumer();
    assertTrue(producer.isStopped());
    assertTrue(consumer.isStopped());
  }
}
