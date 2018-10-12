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
package org.apache.samza.system.descriptors;

import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.system.descriptors.GenericOutputDescriptor;
import org.apache.samza.system.descriptors.GenericSystemDescriptor;
import org.apache.samza.serializers.DoubleSerde;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.system.SystemStreamMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestGenericInputDescriptor {
  @Test
  public void testAPIUsage() {
    // does not assert anything, but acts as a compile-time check on expected descriptor type parameters
    // and validates that the method calls can be chained.
    GenericSystemDescriptor mySystem =
        new GenericSystemDescriptor("input-system", "factory.class.name")
            .withSystemConfigs(Collections.emptyMap())
            .withDefaultStreamConfigs(Collections.emptyMap());
    GenericInputDescriptor<Integer> input1 = mySystem.getInputDescriptor("input1", new IntegerSerde());
    GenericOutputDescriptor<Integer> output1 = mySystem.getOutputDescriptor("output1", new IntegerSerde());

    input1
        .withPhysicalName("input-1")
        .shouldBootstrap()
        .withOffsetDefault(SystemStreamMetadata.OffsetType.NEWEST)
        .withPriority(1)
        .shouldResetOffset()
        .isBounded()
        .shouldDeleteCommittedMessages()
        .withStreamConfigs(Collections.emptyMap());

    output1
        .withPhysicalName("output-1")
        .withStreamConfigs(Collections.emptyMap());
  }


  @Test
  public void testISDConfigsWithOverrides() {
    GenericSystemDescriptor mySystem =
        new GenericSystemDescriptor("input-system", "factory.class.name")
            .withSystemConfigs(Collections.emptyMap())
            .withDefaultStreamConfigs(Collections.emptyMap());

    GenericInputDescriptor<Double> isd = mySystem.getInputDescriptor("input-stream", new DoubleSerde())
            .withPhysicalName("physical-name")
            .shouldBootstrap()
            .isBounded()
            .shouldDeleteCommittedMessages()
            .withOffsetDefault(SystemStreamMetadata.OffsetType.OLDEST)
            .withPriority(12)
            .shouldResetOffset()
            .withStreamConfigs(ImmutableMap.of("custom-config-key", "custom-config-value"));

    Map<String, String> generatedConfigs = isd.toConfig();
    Map<String, String> expectedConfigs = new HashMap<>();
    expectedConfigs.put("streams.input-stream.samza.system", "input-system");
    expectedConfigs.put("streams.input-stream.samza.physical.name", "physical-name");
    expectedConfigs.put("streams.input-stream.samza.bootstrap", "true");
    expectedConfigs.put("streams.input-stream.samza.bounded", "true");
    expectedConfigs.put("streams.input-stream.samza.delete.committed.messages", "true");
    expectedConfigs.put("streams.input-stream.samza.reset.offset", "true");
    expectedConfigs.put("streams.input-stream.samza.offset.default", "oldest");
    expectedConfigs.put("streams.input-stream.samza.priority", "12");
    expectedConfigs.put("streams.input-stream.custom-config-key", "custom-config-value");

    assertEquals(expectedConfigs, generatedConfigs);
  }

  @Test
  public void testISDConfigsWithDefaults() {
    GenericSystemDescriptor mySystem =
        new GenericSystemDescriptor("input-system", "factory.class.name")
            .withSystemConfigs(Collections.emptyMap())
            .withDefaultStreamConfigs(Collections.emptyMap());

    DoubleSerde streamSerde = new DoubleSerde();
    GenericInputDescriptor<Double> isd = mySystem.getInputDescriptor("input-stream", streamSerde);

    Map<String, String> generatedConfigs = isd.toConfig();
    Map<String, String> expectedConfigs = ImmutableMap.of("streams.input-stream.samza.system", "input-system");
    assertEquals(expectedConfigs, generatedConfigs);
    assertEquals(streamSerde, isd.getSerde());
    assertFalse(isd.getTransformer().isPresent());
  }

  @Test
  public void testISDObjectsWithOverrides() {
    GenericSystemDescriptor mySystem =
        new GenericSystemDescriptor("input-system", "factory.class.name")
            .withSystemConfigs(Collections.emptyMap())
            .withDefaultStreamConfigs(Collections.emptyMap());
    IntegerSerde streamSerde = new IntegerSerde();
    GenericInputDescriptor<Integer> isd = mySystem.getInputDescriptor("input-stream", streamSerde);

    assertEquals(streamSerde, isd.getSerde());
    assertFalse(isd.getTransformer().isPresent());
  }
}
