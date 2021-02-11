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
package org.apache.samza.system.kafka.descriptors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import org.apache.samza.system.SystemStreamMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestKafkaSystemDescriptor {
  @Test
  public void testSDConfigsWithOverrides() {
    KafkaSystemDescriptor sd =
        new KafkaSystemDescriptor("kafka")
            .withConsumerZkConnect(ImmutableList.of("localhost:1234"))
            .withProducerBootstrapServers(ImmutableList.of("localhost:567", "localhost:890"))
            .withDefaultStreamOffsetDefault(SystemStreamMetadata.OffsetType.OLDEST)
            .withConsumerAutoOffsetReset("smallest")
            .withConsumerFetchMessageMaxBytes(1024 * 1024)
            .withSamzaFetchThreshold(10000)
            .withSamzaFetchThresholdBytes(1024 * 1024)
            .withConsumerConfigs(ImmutableMap.of("custom-consumer-config-key", "custom-consumer-config-value"))
            .withProducerConfigs(ImmutableMap.of("custom-producer-config-key", "custom-producer-config-value"))
            .withDefaultStreamConfigs(ImmutableMap.of("custom-stream-config-key", "custom-stream-config-value"));

    Map<String, String> generatedConfigs = sd.toConfig();
    assertEquals("org.apache.samza.system.kafka.KafkaSystemFactory", generatedConfigs.get("systems.kafka.samza.factory"));
    assertEquals("localhost:1234", generatedConfigs.get("systems.kafka.consumer.zookeeper.connect"));
    assertEquals("localhost:567,localhost:890", generatedConfigs.get("systems.kafka.producer.bootstrap.servers"));
    assertEquals("smallest", generatedConfigs.get("systems.kafka.consumer.auto.offset.reset"));
    assertEquals("1048576", generatedConfigs.get("systems.kafka.consumer.fetch.message.max.bytes"));
    assertEquals("10000", generatedConfigs.get("systems.kafka.samza.fetch.threshold"));
    assertEquals("1048576", generatedConfigs.get("systems.kafka.samza.fetch.threshold.bytes"));
    assertEquals("custom-consumer-config-value", generatedConfigs.get("systems.kafka.consumer.custom-consumer-config-key"));
    assertEquals("custom-producer-config-value", generatedConfigs.get("systems.kafka.producer.custom-producer-config-key"));
    assertEquals("custom-stream-config-value", generatedConfigs.get("systems.kafka.default.stream.custom-stream-config-key"));
    assertEquals("oldest", generatedConfigs.get("systems.kafka.default.stream.samza.offset.default"));
    assertEquals(11, generatedConfigs.size());
  }

  @Test
  public void testSDConfigsWithoutOverrides() {
    KafkaSystemDescriptor sd = new KafkaSystemDescriptor("kafka");

    Map<String, String> generatedConfigs = sd.toConfig();
    assertEquals("org.apache.samza.system.kafka.KafkaSystemFactory", generatedConfigs.get("systems.kafka.samza.factory"));
    assertEquals(1, generatedConfigs.size()); // verify that there are no other configs
  }
}
