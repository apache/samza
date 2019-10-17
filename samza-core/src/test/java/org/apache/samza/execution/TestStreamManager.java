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

package org.apache.samza.execution;

import org.apache.samza.Partition;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStreamMetadata;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class TestStreamManager {
  private static final String SYSTEM1 = "system-1";
  private static final String SYSTEM2 = "system-2";
  private static final String STREAM1 = "stream-1";
  private static final String STREAM2 = "stream-2";

  @Test
  public void testCreateStreams() {
    StreamSpec spec1 = new StreamSpec(STREAM1, STREAM1, SYSTEM1);
    StreamSpec spec2 = new StreamSpec(STREAM2, STREAM2, SYSTEM2);
    List<StreamSpec> specList = new ArrayList<>();
    specList.add(spec1);
    specList.add(spec2);

    SystemAdmin admin1 = mock(SystemAdmin.class);
    SystemAdmin admin2 = mock(SystemAdmin.class);
    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    when(systemAdmins.getSystemAdmin(SYSTEM1)).thenReturn(admin1);
    when(systemAdmins.getSystemAdmin(SYSTEM2)).thenReturn(admin2);
    StreamManager manager = new StreamManager(systemAdmins);
    manager.createStreams(specList);

    ArgumentCaptor<StreamSpec> captor = ArgumentCaptor.forClass(StreamSpec.class);
    verify(admin1).createStream(captor.capture());
    assertEquals(STREAM1, captor.getValue().getPhysicalName());

    captor = ArgumentCaptor.forClass(StreamSpec.class);
    verify(admin2).createStream(captor.capture());
    assertEquals(STREAM2, captor.getValue().getPhysicalName());
  }

  @Test
  public void testGetStreamPartitionCounts() {
    SystemAdmin admin1 = mock(SystemAdmin.class);
    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    when(systemAdmins.getSystemAdmin(SYSTEM1)).thenReturn(admin1);

    Map<String, SystemStreamMetadata> map = new HashMap<>();
    SystemStreamMetadata meta1 = mock(SystemStreamMetadata.class);
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitions = new HashMap<>();
    partitions.put(new Partition(0), null);
    when(meta1.getSystemStreamPartitionMetadata()).thenReturn(partitions);
    map.put(STREAM1, meta1);

    SystemStreamMetadata meta2 = mock(SystemStreamMetadata.class);
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitions2 = new HashMap<>();
    partitions2.put(new Partition(0), null);
    partitions2.put(new Partition(1), null);
    when(meta2.getSystemStreamPartitionMetadata()).thenReturn(partitions2);
    map.put(STREAM2, meta2);

    when(admin1.getSystemStreamMetadata(anyObject())).thenReturn(map);

    Set<String> streams = new HashSet<>();
    streams.add(STREAM1);
    streams.add(STREAM2);
    StreamManager manager = new StreamManager(systemAdmins);
    Map<String, Integer> counts = manager.getStreamPartitionCounts(SYSTEM1, streams);

    assertTrue(counts.get(STREAM1).equals(1));
    assertTrue(counts.get(STREAM2).equals(2));
  }

  private static CheckpointManager checkpointManager = mock(CheckpointManager.class);
  public static final class MockCheckpointManagerFactory implements CheckpointManagerFactory {
    @Override
    public CheckpointManager getCheckpointManager(Config config, MetricsRegistry registry) {
      return checkpointManager;
    }
  }

  @Test
  public void testClearStreamsFromPreviousRun() {
    SystemAdmin admin1 = mock(SystemAdmin.class);
    SystemAdmin admin2 = mock(SystemAdmin.class);
    SystemAdmins systemAdmins = mock(SystemAdmins.class);
    when(systemAdmins.getSystemAdmin(SYSTEM1)).thenReturn(admin1);
    when(systemAdmins.getSystemAdmin(SYSTEM2)).thenReturn(admin2);

    String runId = "123";
    Map<String, String> config = new HashMap<>();
    config.put(ApplicationConfig.APP_RUN_ID, "123");
    config.put(ApplicationConfig.APP_MODE, ApplicationConfig.ApplicationMode.BATCH.name());

    config.put("streams.stream-1.samza.system", SYSTEM1);
    config.put("streams.stream-1.samza.physical.name", STREAM1 + "-" + runId);
    config.put("streams.stream-1.samza.intermediate", "true");

    config.put("task.checkpoint.factory", MockCheckpointManagerFactory.class.getName());
    config.put("stores.test-store.factory", "dummyfactory");
    config.put("stores.test-store.changelog", SYSTEM2 + "." + STREAM2);

    StreamManager manager = new StreamManager(systemAdmins);
    manager.clearStreamsFromPreviousRun(new MapConfig(config));

    ArgumentCaptor<StreamSpec> captor = ArgumentCaptor.forClass(StreamSpec.class);
    verify(admin1).clearStream(captor.capture());
    assertEquals(captor.getValue().getPhysicalName(), STREAM1 + "-" + runId);

    captor = ArgumentCaptor.forClass(StreamSpec.class);
    verify(admin2).clearStream(captor.capture());
    assertEquals(captor.getValue().getPhysicalName(), STREAM2 + "-" + runId);

    verify(checkpointManager, times(1)).clearCheckpoints();
  }
}
