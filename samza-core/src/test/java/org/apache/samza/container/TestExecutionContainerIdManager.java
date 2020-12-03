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
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStoreTestUtil;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory;
import org.apache.samza.coordinator.stream.messages.SetExecutionEnvContainerIdMapping;
import org.apache.samza.metadatastore.MetadataStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestExecutionContainerIdManager {

  private static final Config
      CONFIG = new MapConfig(ImmutableMap.of("job.name", "test-job", "job.coordinator.system", "test-kafka"));

  private CoordinatorStreamStore coordinatorStreamStore;
  private CoordinatorStreamStoreTestUtil coordinatorStreamStoreTestUtil;
  private MetadataStore store;
  private ExecutionContainerIdManager executionContainerIdManager;

  @Before
  public void setup() {
    coordinatorStreamStoreTestUtil = new CoordinatorStreamStoreTestUtil(CONFIG);
    coordinatorStreamStore = coordinatorStreamStoreTestUtil.getCoordinatorStreamStore();
    store = Mockito.spy(new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore,
        SetExecutionEnvContainerIdMapping.TYPE));
    executionContainerIdManager = new ExecutionContainerIdManager(store);

  }

  @After
  public void tearDown() {
    MockCoordinatorStreamSystemFactory.disableMockConsumerCache();
  }

  @Test
  public void testExecutionContainerIdManager() {
    String physicalId = "container_123_123_123";
    String processorId = "0";

    executionContainerIdManager.writeExecutionEnvironmentContainerIdMapping(processorId, physicalId);
    Map<String, String> localMap = executionContainerIdManager.readExecutionEnvironmentContainerIdMapping();

    Map<String, String> expectedMap = ImmutableMap.of(processorId, physicalId);
    assertEquals(expectedMap, localMap);

    executionContainerIdManager.close();

    MockCoordinatorStreamSystemFactory.MockCoordinatorStreamSystemProducer producer = coordinatorStreamStoreTestUtil.getMockCoordinatorStreamSystemProducer();
    MockCoordinatorStreamSystemFactory.MockCoordinatorStreamSystemConsumer consumer = coordinatorStreamStoreTestUtil.getMockCoordinatorStreamSystemConsumer();
    assertTrue(producer.isStopped());
    assertTrue(consumer.isStopped());

    ArgumentCaptor<byte[]> argument1 = ArgumentCaptor.forClass(byte[].class);
    Mockito.verify(store).put(Mockito.eq(processorId), argument1.capture());
    CoordinatorStreamValueSerde valueSerde = new CoordinatorStreamValueSerde(SetExecutionEnvContainerIdMapping.TYPE);
    assertEquals(physicalId, valueSerde.fromBytes(argument1.getValue()));
  }

  @Test(expected = NullPointerException.class)
  public void testInvalidKeyExecutionContainerIdManager() {
    String physicalId = "container_123_123_123";
    String processorId = null;
    executionContainerIdManager.writeExecutionEnvironmentContainerIdMapping(processorId, physicalId);
  }
  @Test(expected = NullPointerException.class)
  public void testInvalidValueExecutionContainerIdManager() {
    String physicalId = null;
    String processorId = "0";
    executionContainerIdManager.writeExecutionEnvironmentContainerIdMapping(processorId, physicalId);
  }
}

