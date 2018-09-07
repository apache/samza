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
package org.apache.samza.system.kafka;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.Map;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestKafkaInputDescriptor {
  @Test
  public void testISDConfigsWithOverrides() {
    KafkaSystemDescriptor sd = new KafkaSystemDescriptor("kafka");

    KafkaInputDescriptor<KV<String, Integer>> isd =
        sd.getInputDescriptor("input-stream", KVSerde.of(new StringSerde(), new IntegerSerde()))
            .withPhysicalName("physical-name")
            .withConsumerAutoOffsetReset("largest")
            .withConsumerFetchMessageMaxBytes(1024 * 1024);

    Map<String, String> generatedConfigs = isd.toConfig(Collections.emptyMap());
    assertEquals("kafka", generatedConfigs.get("streams.input-stream.samza.system"));
    assertEquals("physical-name", generatedConfigs.get("streams.input-stream.samza.physical.name"));
    assertEquals("largest", generatedConfigs.get("systems.kafka.streams.physical-name.consumer.auto.offset.reset"));
    assertEquals("1048576", generatedConfigs.get("systems.kafka.streams.physical-name.consumer.fetch.message.max.bytes"));
  }

  @Test
  public void testISDConfigsWithDefaults() {
    KafkaSystemDescriptor sd = new KafkaSystemDescriptor("kafka")
        .withConsumerZkConnect(ImmutableList.of("localhost:123"))
        .withProducerBootstrapServers(ImmutableList.of("localhost:567", "localhost:890"));

    KafkaInputDescriptor<KV<String, Integer>> isd =
        sd.getInputDescriptor("input-stream", KVSerde.of(new StringSerde(), new IntegerSerde()));

    Map<String, String> generatedConfigs = isd.toConfig(Collections.emptyMap());
    assertEquals("kafka", generatedConfigs.get("streams.input-stream.samza.system"));
    assertEquals(1, generatedConfigs.size()); // verify that there are no other configs
  }
}
