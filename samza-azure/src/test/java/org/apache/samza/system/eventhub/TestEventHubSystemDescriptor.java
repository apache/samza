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
package org.apache.samza.system.eventhub;

import java.util.Map;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer.PartitioningMethod;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestEventHubSystemDescriptor {
  @Test
  public void testWithDescriptorOverrides() {
    String systemName = "system-name";
    String streamId1 = "input-stream1";
    String streamId2 = "input-stream2";
    String streamId3 = "output-stream1";
    String streamId4 = "output-stream2";

    EventHubSystemDescriptor systemDescriptor = new EventHubSystemDescriptor(systemName)
        .withMaxEventCountPerPoll(1000)
        .withNumClientThreads(5)
        .withPartitioningMethod(PartitioningMethod.PARTITION_KEY_AS_PARTITION)
        .withPrefetchCount(100)
        .withReceiveQueueSize(500)
        .withRuntimeInfoTimeout(60000)
        .withSendKeys(false);

    systemDescriptor.getInputDescriptor(streamId1, KVSerde.of(new StringSerde(),
        new IntegerSerde()), "entity-namespace1", "entity1");
    systemDescriptor.getInputDescriptor(streamId2, KVSerde.of(new StringSerde(),
            new IntegerSerde()), "entity-namespace2", "entity2");
    systemDescriptor.getOutputDescriptor(streamId3, KVSerde.of(new StringSerde(),
            new IntegerSerde()), "entity-namespace3", "entity3");
    systemDescriptor.getOutputDescriptor(streamId4, KVSerde.of(new StringSerde(),
            new IntegerSerde()), "entity-namespace4", "entity4");

    Map<String, String> generatedConfigs = systemDescriptor.toConfig();
    assertEquals("org.apache.samza.system.eventhub.EventHubSystemFactory", generatedConfigs.get(String.format("systems.%s.samza.factory", systemName)));
    assertEquals("1000", generatedConfigs.get(String.format(EventHubConfig.CONFIG_MAX_EVENT_COUNT_PER_POLL, systemName)));
    assertEquals("5", generatedConfigs.get(String.format(EventHubConfig.CONFIG_SYSTEM_NUM_CLIENT_THREADS, systemName)));
    assertEquals("PARTITION_KEY_AS_PARTITION", generatedConfigs.get(String.format(EventHubConfig.CONFIG_PRODUCER_PARTITION_METHOD, systemName)));
    assertEquals("100", generatedConfigs.get(String.format(EventHubConfig.CONFIG_PREFETCH_COUNT, systemName)));
    assertEquals("500", generatedConfigs.get(String.format(EventHubConfig.CONFIG_CONSUMER_BUFFER_CAPACITY, systemName)));
    assertEquals("60000", generatedConfigs.get(String.format(EventHubConfig.CONFIG_FETCH_RUNTIME_INFO_TIMEOUT_MILLIS, systemName)));
    assertEquals("false", generatedConfigs.get(String.format(EventHubConfig.CONFIG_SEND_KEY_IN_EVENT_PROPERTIES, systemName)));
    assertEquals(streamId1 + "," + streamId2 + "," + streamId3 + "," + streamId4, generatedConfigs.get(String.format(EventHubConfig.CONFIG_STREAM_LIST, systemName)));
  }

  @Test
  public void testWithoutDescriptorOverrides() {
    String systemName = "eventHub";

    EventHubSystemDescriptor systemDescriptor = new EventHubSystemDescriptor(systemName);

    Map<String, String> generatedConfigs = systemDescriptor.toConfig();
    assertEquals("org.apache.samza.system.eventhub.EventHubSystemFactory", generatedConfigs.get(String.format("systems.%s.samza.factory", systemName)));
    assertNull(generatedConfigs.get(String.format(EventHubConfig.CONFIG_MAX_EVENT_COUNT_PER_POLL, systemName)));
    assertNull(generatedConfigs.get(String.format(EventHubConfig.CONFIG_SYSTEM_NUM_CLIENT_THREADS, systemName)));
    assertNull(generatedConfigs.get(String.format(EventHubConfig.CONFIG_PRODUCER_PARTITION_METHOD, systemName)));
    assertNull(generatedConfigs.get(String.format(EventHubConfig.CONFIG_PREFETCH_COUNT, systemName)));
    assertNull(generatedConfigs.get(String.format(EventHubConfig.CONFIG_CONSUMER_BUFFER_CAPACITY, systemName)));
    assertNull(generatedConfigs.get(String.format(EventHubConfig.CONFIG_FETCH_RUNTIME_INFO_TIMEOUT_MILLIS, systemName)));
    assertNull(generatedConfigs.get(String.format(EventHubConfig.CONFIG_SEND_KEY_IN_EVENT_PROPERTIES, systemName)));
    assertNull(generatedConfigs.get(String.format(EventHubConfig.CONFIG_STREAM_LIST, systemName)));
    assertEquals(1, generatedConfigs.size());
  }
  @Test
  public void testWithInputOutputStreams() {
    String systemName = "system-name";
    String streamId1 = "input-stream1";
    String streamId2 = "input-stream2";
    String streamId3 = "output-stream1";
    String streamId4 = "output-stream2";

    EventHubSystemDescriptor systemDescriptor = new EventHubSystemDescriptor(systemName);

    systemDescriptor.getInputDescriptor(streamId1, KVSerde.of(new StringSerde(),
        new IntegerSerde()), "entity-namespace1", "entity1");
    systemDescriptor.getInputDescriptor(streamId2, KVSerde.of(new StringSerde(),
        new IntegerSerde()), "entity-namespace2", "entity2");
    systemDescriptor.getOutputDescriptor(streamId3, KVSerde.of(new StringSerde(),
        new IntegerSerde()), "entity-namespace3", "entity3");
    systemDescriptor.getOutputDescriptor(streamId4, KVSerde.of(new StringSerde(),
        new IntegerSerde()), "entity-namespace4", "entity4");

    Map<String, String> generatedConfigs = systemDescriptor.toConfig();
    assertEquals("org.apache.samza.system.eventhub.EventHubSystemFactory", generatedConfigs.get(String.format("systems.%s.samza.factory", systemName)));
    assertEquals(streamId1 + "," + streamId2 + "," + streamId3 + "," + streamId4, generatedConfigs.get(String.format(EventHubConfig.CONFIG_STREAM_LIST, systemName)));
    assertEquals(2, generatedConfigs.size());
  }
}
