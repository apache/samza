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

package org.apache.samza.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory;
import org.apache.samza.serializers.ByteSerdeFactory;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.MockSystemFactory;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestStorageRecovery {

  public Config config = null;
  String path = "/tmp/testing";
  private static final String SYSTEM_STREAM_NAME = "changelog";
  private static final String INPUT_STREAM = "input";
  private static final String STORE_NAME = "testStore";
  public static SystemStreamPartition ssp = new SystemStreamPartition("mockSystem", SYSTEM_STREAM_NAME, new Partition(0));
  public static IncomingMessageEnvelope msg = new IncomingMessageEnvelope(ssp, "0", "test", "test");

  @Before
  public void setup() throws InterruptedException {
    putConfig();
    putMetadata();
  }

  @After
  public void teardown() {
    MockCoordinatorStreamSystemFactory.disableMockConsumerCache();
  }

  @Test
  public void testStorageEngineReceivedAllValues() {
    MockCoordinatorStreamSystemFactory.enableMockConsumerCache();


    StorageRecovery storageRecovery = new StorageRecovery(config, path);
    storageRecovery.run();

    // because the stream has two partitions
    assertEquals(2, MockStorageEngine.incomingMessageEnvelopes.size());
    assertEquals(msg, MockStorageEngine.incomingMessageEnvelopes.get(0));
    assertEquals(msg, MockStorageEngine.incomingMessageEnvelopes.get(1));
    // correct path is passed to the store engine
    String expectedStoreDir = String.format("%s/state/%s/Partition_", path, STORE_NAME);
    String actualStoreDir = MockStorageEngine.storeDir.toString();
    assertEquals(expectedStoreDir, actualStoreDir.substring(0, actualStoreDir.length() - 1));
  }

  private void putConfig() {
    Map<String, String> map = new HashMap<String, String>();
    map.put("job.name", "changelogTest");
    map.put("systems.mockSystem.samza.factory", MockSystemFactory.class.getCanonicalName());
    map.put(String.format("stores.%s.factory", STORE_NAME), MockStorageEngineFactory.class.getCanonicalName());
    map.put(String.format("stores.%s.changelog", STORE_NAME), "mockSystem." + SYSTEM_STREAM_NAME);
    map.put(String.format("stores.%s.key.serde", STORE_NAME), "byteserde");
    map.put(String.format("stores.%s.msg.serde", STORE_NAME), "byteserde");
    map.put("serializers.registry.byteserde.class", ByteSerdeFactory.class.getName());
    map.put("task.inputs", "mockSystem.input");
    map.put("job.coordinator.system", "coordinator");
    map.put("systems.coordinator.samza.factory", MockCoordinatorStreamSystemFactory.class.getCanonicalName());
    map.put("task.name.grouper.factory", "org.apache.samza.container.grouper.task.GroupByContainerCountFactory");
    config = new MapConfig(map);
  }

  private void putMetadata() {
    MockSystemFactory.MSG_QUEUES.put(new SystemStreamPartition("mockSystem", SYSTEM_STREAM_NAME, new Partition(0)), Collections.singletonList(msg));
    MockSystemFactory.MSG_QUEUES.put(new SystemStreamPartition("mockSystem", SYSTEM_STREAM_NAME, new Partition(1)), Collections.singletonList(msg));
    MockSystemFactory.MSG_QUEUES.put(new SystemStreamPartition("mockSystem", INPUT_STREAM, new Partition(0)), new ArrayList<>());
    MockSystemFactory.MSG_QUEUES.put(new SystemStreamPartition("mockSystem", INPUT_STREAM, new Partition(1)), new ArrayList<>());
  }
}
