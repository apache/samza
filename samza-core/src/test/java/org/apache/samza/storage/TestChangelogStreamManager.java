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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetChangelogMapping;
import org.apache.samza.metadatastore.InMemoryMetadataStore;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.StreamValidationException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;


public class TestChangelogStreamManager {
  private static final String SYSTEM = "system";
  private static final String STREAM = "stream";
  private static final int MAX_CHANGELOG_STREAM_PARTITIONS = 10;
  private static final Set<StreamSpec> EXPECTED_STREAM_SPECS = ImmutableSet.of(
      // changelog
      StreamSpec.createChangeLogStreamSpec(STREAM, SYSTEM, MAX_CHANGELOG_STREAM_PARTITIONS),
      // access log
      new StreamSpec(STREAM + "-access-log", STREAM + "-access-log", SYSTEM, MAX_CHANGELOG_STREAM_PARTITIONS));

  @Test
  public void createChangelogStreams() {
    Map<String, String> map = new ImmutableMap.Builder<String, String>()
        .put("stores.store0.factory", "factory.class")
        .put("stores.store0.changelog", SYSTEM + "." + STREAM)
        .put("stores.store0.accesslog.enabled", "true")
        .put(String.format("systems.%s.samza.factory", SYSTEM), MockSystemAdminFactory.class.getName())
        .build();
    Config config = new MapConfig(map);
    ChangelogStreamManager.createChangelogStreams(config, MAX_CHANGELOG_STREAM_PARTITIONS);
  }

  @Test
  public void testWritePartitionMapping() {
    InMemoryMetadataStore inMemoryMetadataStore = new InMemoryMetadataStore();
    NamespaceAwareCoordinatorStreamStore prevChangelogStreamStore =
        new NamespaceAwareCoordinatorStreamStore(inMemoryMetadataStore, SetChangelogMapping.TYPE);
    CoordinatorStreamValueSerde valueSerde = new CoordinatorStreamValueSerde(SetChangelogMapping.TYPE);
    TaskName t0 = new TaskName("Task0");
    TaskName t1 = new TaskName("Task1");
    TaskName t2 = new TaskName("Task2");
    TaskName t3 = new TaskName("Task3");
    TaskName t4 = new TaskName("Task4");

    prevChangelogStreamStore.put(t0.getTaskName(), valueSerde.toBytes("0"));
    prevChangelogStreamStore.put(t1.getTaskName(), valueSerde.toBytes("1"));
    prevChangelogStreamStore.put(t2.getTaskName(), valueSerde.toBytes("2"));
    prevChangelogStreamStore.put(t3.getTaskName(), valueSerde.toBytes("3"));
    prevChangelogStreamStore.put(t4.getTaskName(), valueSerde.toBytes("4"));

    NamespaceAwareCoordinatorStreamStore currentChangelogStreamStore =
        spy(new NamespaceAwareCoordinatorStreamStore(inMemoryMetadataStore, SetChangelogMapping.TYPE));
    ChangelogStreamManager changelogStreamManager = new ChangelogStreamManager(currentChangelogStreamStore);

    HashMap<TaskName, Integer> newPartitionMapping = new HashMap<>();
    newPartitionMapping.put(t0, 5);
    newPartitionMapping.put(t1, 6);
    newPartitionMapping.put(t2, null); // delete
    newPartitionMapping.put(t3, null); // delete
    newPartitionMapping.put(t4, 7);

    changelogStreamManager.writePartitionMapping(newPartitionMapping);

    verify(currentChangelogStreamStore).delete(t2.getTaskName());
    verify(currentChangelogStreamStore).delete(t3.getTaskName());
    ArgumentCaptor<Map> argumentsCaptured = ArgumentCaptor.forClass(Map.class);
    verify(currentChangelogStreamStore).putAll((Map<String, byte[]>) argumentsCaptured.capture());
    assertEquals(ImmutableSet.of(t0.getTaskName(), t1.getTaskName(), t4.getTaskName()), argumentsCaptured.getValue().keySet());
  }

  public static class MockSystemAdminFactory implements SystemFactory {
    @Override
    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
      throw new UnsupportedOperationException("Unused in test");
    }

    @Override
    public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
      throw new UnsupportedOperationException("Unused in test");
    }

    @Override
    public SystemAdmin getAdmin(String systemName, Config config) {
      return new MockSystemAdmin();
    }
  }

  private static class MockSystemAdmin implements SystemAdmin {
    private final Set<StreamSpec> streamsCreated = new HashSet<>();
    private final Set<StreamSpec> streamsValidated = new HashSet<>();

    @Override
    public boolean createStream(StreamSpec streamSpec) {
      assertTrue(EXPECTED_STREAM_SPECS.contains(streamSpec));
      this.streamsCreated.add(streamSpec);
      return true;
    }

    @Override
    public void validateStream(StreamSpec streamSpec) {
      if (!EXPECTED_STREAM_SPECS.contains(streamSpec)) {
        throw new StreamValidationException("Did not see expected stream spec");
      }
      this.streamsValidated.add(streamSpec);
    }

    @Override
    public void stop() {
      assertEquals(EXPECTED_STREAM_SPECS, this.streamsCreated);
      assertEquals(EXPECTED_STREAM_SPECS, this.streamsValidated);
    }

    @Override
    public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
      throw new UnsupportedOperationException("Unused in test");
    }

    @Override
    public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
      throw new UnsupportedOperationException("Unused in test");
    }

    @Override
    public Integer offsetComparator(String offset1, String offset2) {
      throw new UnsupportedOperationException("Unused in test");
    }
  }
}
