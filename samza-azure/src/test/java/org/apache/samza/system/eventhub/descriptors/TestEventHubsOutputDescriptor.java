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
package org.apache.samza.system.eventhub.descriptors;

import java.util.Map;
import org.apache.samza.config.ConfigException;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TestEventHubsOutputDescriptor {
  @Test
  public void testEntityConnectionConfigs() {
    String systemName = "eventHub";
    String streamId = "output-stream";

    EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor(systemName);

    EventHubsOutputDescriptor<KV<String, Integer>> outputDescriptor = systemDescriptor
        .getOutputDescriptor(streamId, "entity-namespace", "entity3", KVSerde.of(new StringSerde(), new IntegerSerde()))
        .withSasKeyName("secretkey")
        .withSasKey("sasToken-123");

    Map<String, String> generatedConfigs = outputDescriptor.toConfig();
    assertEquals("eventHub", generatedConfigs.get("streams.output-stream.samza.system"));
    assertEquals("entity-namespace", generatedConfigs.get(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, streamId)));
    assertEquals("entity3", generatedConfigs.get(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, streamId)));
    assertEquals("secretkey", generatedConfigs.get(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, streamId)));
    assertEquals("sasToken-123", generatedConfigs.get(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, streamId)));
  }

  @Test
  public void testWithoutEntityConnectionConfigs() {
    String systemName = "eventHub";
    String streamId = "output-stream";

    EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor(systemName);

    EventHubsOutputDescriptor<KV<String, Integer>> outputDescriptor = systemDescriptor
        .getOutputDescriptor(streamId, "entity-namespace", "entity3", KVSerde.of(new StringSerde(), new IntegerSerde()));

    Map<String, String> generatedConfigs = outputDescriptor.toConfig();
    assertEquals("eventHub", generatedConfigs.get("streams.output-stream.samza.system"));
    assertEquals("entity-namespace", generatedConfigs.get(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, streamId)));
    assertEquals("entity3", generatedConfigs.get(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, streamId)));
    assertNull(generatedConfigs.get(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, streamId)));
    assertNull(generatedConfigs.get(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, streamId)));
    assertNull(generatedConfigs.get(String.format(EventHubConfig.CONFIG_STREAM_CONSUMER_GROUP, streamId)));
    assertEquals(3, generatedConfigs.size()); // verify that there are no other configs
  }

  @Test
  public void testMissingOutputDescriptorFields() {
    String systemName = "eventHub";
    String streamId = "input-stream";

    EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor(systemName);
    try {
      systemDescriptor.getOutputDescriptor(streamId, null, null, KVSerde.of(new StringSerde(), new IntegerSerde()));
      fail("Should have thrown Config Exception");
    } catch (ConfigException exception) {
      assertEquals(String.format("Missing namespace and entity path Event Hubs output descriptor in " //
          + "system: {%s}, stream: {%s}", systemName, streamId), exception.getMessage());
    }
  }
}
