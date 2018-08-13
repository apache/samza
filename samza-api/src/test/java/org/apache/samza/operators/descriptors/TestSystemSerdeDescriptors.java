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
import org.apache.samza.operators.descriptors.serde.MockSimpleInputDescriptor;
import org.apache.samza.operators.descriptors.serde.MockSimpleOutputDescriptor;
import org.apache.samza.operators.descriptors.serde.MockSimpleSystemDescriptor;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.StringSerde;
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
    MockSimpleSystemDescriptor kafkaSystem =
        new MockSimpleSystemDescriptor("kafka-system")
            .withSystemConfigs(Collections.emptyMap());

    MockSimpleInputDescriptor<Integer> input1 = kafkaSystem.getInputDescriptor("input1", new IntegerSerde());
    MockSimpleInputDescriptor<Float> input2 = kafkaSystem.getInputDescriptor("input2", ime -> 1f, new IntegerSerde());

    MockSimpleOutputDescriptor<Integer> output1 = kafkaSystem.getOutputDescriptor("output1", new IntegerSerde());

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
    MockSimpleSystemDescriptor ssd =
        new MockSimpleSystemDescriptor("kafka-system");

    Map<String, String> generatedConfigs = ssd.toConfig();
    assertEquals("org.apache.kafka.KafkaSystemFactory", generatedConfigs.get("systems.kafka-system.samza.factory"));
    assertEquals(1, generatedConfigs.size()); // verify that there are no other generated configs
  }

  @Test
  public void testISDObjectsWithOverrides() {
    MockSimpleSystemDescriptor sssd = new MockSimpleSystemDescriptor("kafka-system");

    InputTransformer<IncomingMessageEnvelope> transformer = m -> m;
    IntegerSerde streamSerde = new IntegerSerde();

    MockSimpleInputDescriptor<IncomingMessageEnvelope> isd =
        sssd.getInputDescriptor("input-stream", transformer, streamSerde);

    assertEquals(streamSerde, isd.getSerde());
    assertEquals(transformer, isd.getTransformer().get());
  }

  @Test
  public void testISDObjectsWithDefaults() {
    MockSimpleSystemDescriptor sssd = new MockSimpleSystemDescriptor("kafka-system");
    MockSimpleInputDescriptor<String> isd = sssd.getInputDescriptor("input-stream", new StringSerde());

    assertFalse(isd.getTransformer().isPresent());
  }

  class PageView {

  }
}