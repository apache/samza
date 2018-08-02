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

import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.Map;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.serializers.DoubleSerde;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestGenericDescriptors {
  @Test
  public void testAPIUsage() {
    // does not assert anything, but acts as a compile-time check on expected descriptor type parameters
    // and validates that the method calls can be chained.
    GenericSystemDescriptor<Double> mySystem =
        new GenericSystemDescriptor<>("input-system", "factory.class.name", new DoubleSerde())
            .withSystemConfigs(Collections.emptyMap())
            .withDefaultStreamConfigs(Collections.emptyMap());
    
    GenericInputDescriptor<Double> input1 = mySystem.getInputDescriptor("input1");
    GenericInputDescriptor<Integer> input2 = mySystem.getInputDescriptor("input2", new IntegerSerde());
    GenericInputDescriptor<Float> input3 = mySystem.getInputDescriptor("input3", ime -> 1f);
    GenericInputDescriptor<Float> input4 = mySystem.getInputDescriptor("input4", ime -> 1f, new IntegerSerde());

    GenericInputDescriptor<KV<Integer, String>> input5 =
        GenericInputDescriptor.from("input5", "system", KVSerde.of(new IntegerSerde(), new StringSerde()));

    GenericOutputDescriptor<Double> output1 = mySystem.getOutputDescriptor("output1");
    GenericOutputDescriptor<Integer> output2 = mySystem.getOutputDescriptor("output2", new IntegerSerde());

    input1
        .withPhysicalName("input-1")
        .withBootstrap(false)
        .withOffsetDefault(SystemStreamMetadata.OffsetType.NEWEST)
        .withPriority(1)
        .withResetOffset(false)
        .withBounded(false)
        .withDeleteCommittedMessages(true)
        .withStreamConfigs(Collections.emptyMap());

    output1
        .withPhysicalName("output-1")
        .withStreamConfigs(Collections.emptyMap());
  }

  @Test
  public void testSDConfigs() {
    GenericSystemDescriptor<Double> mySystem =
        new GenericSystemDescriptor<>("input-system", "factory.class.name", new DoubleSerde())
            .withSystemConfigs(ImmutableMap.of("custom-config-key", "custom-config-value"))
            .withDefaultStreamConfigs(ImmutableMap.of("custom-stream-config-key", "custom-stream-config-value"))
            .withDefaultStreamOffsetDefault(SystemStreamMetadata.OffsetType.UPCOMING);

    Map<String, String> generatedConfigs = mySystem.toConfig();
    assertEquals("factory.class.name", generatedConfigs.get("systems.input-system.samza.factory"));
    assertEquals("custom-config-value", generatedConfigs.get("systems.input-system.custom-config-key"));
    assertEquals("custom-stream-config-value", generatedConfigs.get("systems.input-system.default.stream.custom-stream-config-key"));
    assertEquals("upcoming", generatedConfigs.get("systems.input-system.default.stream.samza.offset.default"));
    assertEquals(4, generatedConfigs.size());
  }

  @Test
  public void testISDConfigsWithOverrides() {
    GenericSystemDescriptor<Double> mySystem =
        new GenericSystemDescriptor<>("input-system", "factory.class.name", new DoubleSerde())
            .withSystemConfigs(Collections.emptyMap())
            .withDefaultStreamConfigs(Collections.emptyMap());

    GenericInputDescriptor<Double> isd = mySystem.getInputDescriptor("input-stream")
            .withPhysicalName("physical-name")
            .withBootstrap(true)
            .withBounded(true)
            .withDeleteCommittedMessages(true)
            .withOffsetDefault(SystemStreamMetadata.OffsetType.OLDEST)
            .withPriority(12)
            .withResetOffset(true)
            .withStreamConfigs(ImmutableMap.of("custom-config-key", "custom-config-value"));

    Map<String, String> generatedConfigs = isd.toConfig();
    assertEquals("input-system", generatedConfigs.get("streams.input-stream.samza.system"));
    assertEquals("physical-name", generatedConfigs.get("streams.input-stream.samza.physical.name"));
    assertEquals("true", generatedConfigs.get("streams.input-stream.samza.bootstrap"));
    assertEquals("true", generatedConfigs.get("streams.input-stream.samza.bounded"));
    assertEquals("true", generatedConfigs.get("streams.input-stream.samza.delete.committed.messages"));
    assertEquals("true", generatedConfigs.get("streams.input-stream.samza.reset.offset"));
    assertEquals("oldest", generatedConfigs.get("streams.input-stream.samza.offset.default"));
    assertEquals("12", generatedConfigs.get("streams.input-stream.samza.priority"));
    assertEquals("custom-config-value", generatedConfigs.get("streams.input-stream.custom-config-key"));
  }

  @Test
  public void testISDConfigsWithDefaults() {
    DoubleSerde systemSerde = new DoubleSerde();
    GenericSystemDescriptor<Double> mySystem =
        new GenericSystemDescriptor<>("input-system", "factory.class.name", systemSerde)
            .withSystemConfigs(Collections.emptyMap())
            .withDefaultStreamConfigs(Collections.emptyMap());

    GenericInputDescriptor<Double> isd = mySystem.getInputDescriptor("input-stream");

    Map<String, String> generatedConfigs = isd.toConfig();
    assertEquals("input-system", generatedConfigs.get("streams.input-stream.samza.system"));
    assertEquals(1, generatedConfigs.size()); // verify that there are no other configs
    assertEquals(systemSerde, isd.getSerde());
    assertFalse(isd.getTransformer().isPresent());
  }

  @Test
  public void testISDObjectsWithOverrides() {
    GenericSystemDescriptor<Double> mySystem =
        new GenericSystemDescriptor<>("input-system", "factory.class.name", new DoubleSerde())
            .withSystemConfigs(Collections.emptyMap())
            .withDefaultStreamConfigs(Collections.emptyMap());

    InputTransformer<IncomingMessageEnvelope> transformer = m -> m;
    IntegerSerde streamSerde = new IntegerSerde();

    GenericInputDescriptor<IncomingMessageEnvelope> isd =
        mySystem.getInputDescriptor("input-stream", transformer, streamSerde);

    assertEquals(streamSerde, isd.getSerde());
    assertEquals(transformer, isd.getTransformer().get());
  }

  @Test
  public void testISDObjectsWithDefaults() {
    GenericSystemDescriptor<Double> mySystem =
        new GenericSystemDescriptor<>("input-system", "factory.class.name", new DoubleSerde())
            .withSystemConfigs(Collections.emptyMap())
            .withDefaultStreamConfigs(Collections.emptyMap());

    GenericInputDescriptor<Double> isd = mySystem.getInputDescriptor("input-stream");

    assertEquals(mySystem.getSerde(), isd.getSerde());
    assertFalse(isd.getTransformer().isPresent());
  }
}
