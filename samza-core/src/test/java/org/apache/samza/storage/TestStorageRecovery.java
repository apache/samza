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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestStorageRecovery {

  public static SystemConsumer systemConsumer1 = null;
  public static SystemConsumer systemConsumer2 = null;
  public static SystemAdmin systemAdmin = null;
  public static Config config = null;
  public static SystemStreamMetadata systemStreamMetadata = null;
  public static SystemStreamMetadata inputSystemStreamMetadata = null;
  public final static String SYSTEM_STREAM_NAME = "changelog";
  public final static String INPUT_STREAM = "input";
  public static SystemStreamPartition ssp = new SystemStreamPartition("mockSystem", SYSTEM_STREAM_NAME, new Partition(0));
  public static IncomingMessageEnvelope msg = new IncomingMessageEnvelope(TestStorageRecovery.ssp, "0", "test", "test");

  @Before
  public void setup() throws InterruptedException {
    putConfig();
    putMetadata();

    systemAdmin = mock(SystemAdmin.class);

    Set<String> set1 = new HashSet<String>(Arrays.asList(SYSTEM_STREAM_NAME));
    Set<String> set2 = new HashSet<String>(Arrays.asList(INPUT_STREAM));
    HashMap<String, SystemStreamMetadata> ssmMap = new HashMap<String, SystemStreamMetadata>();
    ssmMap.put(SYSTEM_STREAM_NAME, systemStreamMetadata);
    ssmMap.put(INPUT_STREAM, inputSystemStreamMetadata);
    when(systemAdmin.getSystemStreamMetadata(set1)).thenReturn(ssmMap);
    when(systemAdmin.getSystemStreamMetadata(set2)).thenReturn(ssmMap);
  }

  @After
  public void teardown() {
    MockCoordinatorStreamSystemFactory.disableMockConsumerCache();
  }

  @Test
  public void testStorageEngineReceivedAllValues() {
    MockCoordinatorStreamSystemFactory.enableMockConsumerCache();

    String path = "/tmp/testing";
    StorageRecovery storageRecovery = new StorageRecovery(config, path);
    storageRecovery.run();

    // because the stream has two partitions
    assertEquals(2, MockStorageEngine.incomingMessageEnvelopes.size());
    assertEquals(TestStorageRecovery.msg, MockStorageEngine.incomingMessageEnvelopes.get(0));
    assertEquals(TestStorageRecovery.msg, MockStorageEngine.incomingMessageEnvelopes.get(1));
    // correct path is passed to the store engine
    assertEquals(path + "/state/testStore/Partition_1", MockStorageEngine.storeDir.toString());
  }

  private void putConfig() {
    Map<String, String> map = new HashMap<String, String>();
    map.put("job.name", "changelogTest");
    map.put("systems.mockSystem.samza.factory", MockSystemFactory.class.getCanonicalName());
    map.put("stores.testStore.factory", MockStorageEngineFactory.class.getCanonicalName());
    map.put("stores.testStore.changelog", "mockSystem." + SYSTEM_STREAM_NAME);
    map.put("task.inputs", "mockSystem.input");
    map.put("job.coordinator.system", "coordinator");
    map.put("systems.coordinator.samza.factory", MockCoordinatorStreamSystemFactory.class.getCanonicalName());
    map.put("task.name.grouper.factory", "org.apache.samza.container.grouper.task.GroupByContainerCountFactory");
    config = new MapConfig(map);
  }

  private void putMetadata() {
    SystemStreamMetadata.SystemStreamPartitionMetadata sspm = new SystemStreamMetadata.SystemStreamPartitionMetadata("0", "1", "2");
    HashMap<Partition, SystemStreamPartitionMetadata> map = new HashMap<Partition, SystemStreamPartitionMetadata>();
    map.put(new Partition(0), sspm);
    map.put(new Partition(1), sspm);
    systemStreamMetadata = new SystemStreamMetadata(SYSTEM_STREAM_NAME, map);

    HashMap<Partition, SystemStreamPartitionMetadata> map1 = new HashMap<Partition, SystemStreamPartitionMetadata>();
    map1.put(new Partition(0), sspm);
    map1.put(new Partition(1), sspm);
    inputSystemStreamMetadata = new SystemStreamMetadata(INPUT_STREAM, map1);
  }
}
