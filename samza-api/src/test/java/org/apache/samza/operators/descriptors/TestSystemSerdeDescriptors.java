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
package org.apache.samza.operators.descriptors;

import java.util.Collections;
import java.util.Map;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.descriptors.serde.SystemSerdeInputDescriptor;
import org.apache.samza.operators.descriptors.serde.SystemSerdeOutputDescriptor;
import org.apache.samza.operators.descriptors.serde.SystemSerdeSystemDescriptor;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestSystemSerdeDescriptors {
  @Test
  public void testAPIUsage() {
    // does not assert anything, but acts as a compile-time check on expected descriptor type parameters
    // and validates that the method calls can be chained.
    SystemSerdeSystemDescriptor kafkaSystem =
        new SystemSerdeSystemDescriptor("kafka-system")
            .withSystemConfigs(Collections.emptyMap());

    SystemSerdeInputDescriptor<String> input1 = kafkaSystem.getInputDescriptor("input1");
    SystemSerdeInputDescriptor<Integer> input2 = kafkaSystem.getInputDescriptor("input2", new IntegerSerde());
    SystemSerdeInputDescriptor<Float> input3 = kafkaSystem.getInputDescriptor("input3", ime -> 1f);
    SystemSerdeInputDescriptor<Float> input4 = kafkaSystem.getInputDescriptor("input4", ime -> 1f, new IntegerSerde());

    SystemSerdeInputDescriptor<KV<String, Integer>> input5 = SystemSerdeInputDescriptor.from("input5", "queuing");
    SystemSerdeInputDescriptor<KV<String, PageView>> input6 = SystemSerdeInputDescriptor.from("input6", "queuing", PageView.class);

    SystemSerdeOutputDescriptor<String> output1 = kafkaSystem.getOutputDescriptor("output1");
    SystemSerdeOutputDescriptor<Integer> output2 = kafkaSystem.getOutputDescriptor("output2", new IntegerSerde());

    input1
        .withBootstrap(false)
        .withOffsetDefault(SystemStreamMetadata.OffsetType.NEWEST)
        .withPhysicalName("input-1")
        .withPriority(1)
        .withResetOffset(false)
        .withStreamConfigs(Collections.emptyMap());

    output1
        .withPhysicalName("output-1")
        .withStreamConfigs(Collections.emptyMap());
  }

  @Test
  public void testSDConfigs() {
    SystemSerdeSystemDescriptor sssd =
        new SystemSerdeSystemDescriptor("kafka-system");

    Map<String, String> generatedConfigs = sssd.toConfig();
    assertEquals("org.apache.kafka.KafkaSystemFactory", generatedConfigs.get("systems.kafka-system.samza.factory"));
    assertEquals(1, generatedConfigs.size()); // verify that there are no other generated configs
  }

  @Test
  public void testISDObjectsWithOverrides() {
    SystemSerdeSystemDescriptor sssd = new SystemSerdeSystemDescriptor("kafka-system");

    InputTransformer<IncomingMessageEnvelope> transformer = m -> m;
    IntegerSerde streamSerde = new IntegerSerde();

    SystemSerdeInputDescriptor<IncomingMessageEnvelope> isd =
        sssd.getInputDescriptor("input-stream", transformer, streamSerde);

    assertEquals(streamSerde, isd.getSerde());
    assertEquals(transformer, isd.getTransformer().get());
  }

  @Test
  public void testISDObjectsWithDefaults() {
    SystemSerdeSystemDescriptor sssd = new SystemSerdeSystemDescriptor("kafka-system");
    SystemSerdeInputDescriptor<String> isd = sssd.getInputDescriptor("input-stream");

    assertEquals(sssd.getSerde(), isd.getSerde());
    assertFalse(isd.getTransformer().isPresent());
  }

  class PageView {

  }
}