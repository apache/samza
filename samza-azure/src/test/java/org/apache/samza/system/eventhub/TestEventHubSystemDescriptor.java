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

import com.google.common.collect.ImmutableList;
import java.util.Map;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer.PartitioningMethod;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestEventHubSystemDescriptor {
  @Test
  public void testWithDescriptorOverrides() {
    String systemName = "system-name";

    EventHubSystemDescriptor systemDescriptor = new EventHubSystemDescriptor(systemName)
        .withStreamIds(ImmutableList.of("input-stream1", "input-stream2", "output-stream1", "output-stream2"))
        .withMaxEventCountPerPoll(1000)
        .withNumClientThreads(5)
        .withPartitioningMethod(PartitioningMethod.PARTITION_KEY_AS_PARTITION)
        .withPrefetchCount(100)
        .withReceiveQueueSize(500)
        .withRuntimeInfoTimeout(60000)
        .withSendKeys(false);

    Map<String, String> generatedConfigs = systemDescriptor.toConfig();
    assertEquals("org.apache.samza.system.eventhub.EventHubSystemFactory", generatedConfigs.get(String.format("systems.%s.samza.factory", systemName)));
    assertEquals("1000", generatedConfigs.get(String.format(EventHubConfig.CONFIG_MAX_EVENT_COUNT_PER_POLL, systemName)));
    assertEquals("5", generatedConfigs.get(String.format(EventHubConfig.CONFIG_SYSTEM_NUM_CLIENT_THREADS, systemName)));
    assertEquals("PARTITION_KEY_AS_PARTITION", generatedConfigs.get(String.format(EventHubConfig.CONFIG_PRODUCER_PARTITION_METHOD, systemName)));
    assertEquals("100", generatedConfigs.get(String.format(EventHubConfig.CONFIG_PREFETCH_COUNT, systemName)));
    assertEquals("500", generatedConfigs.get(String.format(EventHubConfig.CONFIG_CONSUMER_BUFFER_CAPACITY, systemName)));
    assertEquals("60000", generatedConfigs.get(String.format(EventHubConfig.CONFIG_FETCH_RUNTIME_INFO_TIMEOUT_MILLIS, systemName)));
    assertEquals("false", generatedConfigs.get(String.format(EventHubConfig.CONFIG_SEND_KEY_IN_EVENT_PROPERTIES, systemName)));
    assertEquals("input-stream1,input-stream2,output-stream1,output-stream2", generatedConfigs.get(String.format(EventHubConfig.CONFIG_STREAM_LIST, systemName)));
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
}
