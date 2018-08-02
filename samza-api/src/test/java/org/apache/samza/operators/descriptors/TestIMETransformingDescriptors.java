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
import org.apache.samza.operators.descriptors.transforming.IMETransformingInputDescriptor;
import org.apache.samza.operators.descriptors.transforming.IMETransformingOutputDescriptor;
import org.apache.samza.operators.descriptors.transforming.IMETransformingSystemDescriptor;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestIMETransformingDescriptors {
  @Test
  public void testAPIUsage() {
    // does not assert anything, but acts as a compile-time check on expected descriptor type parameters
    // and validates that the method calls can be chained.
    IMETransformingSystemDescriptor imeTransformingSystem =
        new IMETransformingSystemDescriptor("imeTransformingSystem")
            .withSystemConfigs(Collections.emptyMap());

    IMETransformingInputDescriptor<Long> input1 = imeTransformingSystem.getInputDescriptor("input1");
    IMETransformingInputDescriptor<Long> input2 = imeTransformingSystem.getInputDescriptor("input2", new IntegerSerde());
    IMETransformingInputDescriptor<Float> input3 = imeTransformingSystem.getInputDescriptor("input3", ime -> 1f);
    IMETransformingInputDescriptor<Float> input4 = imeTransformingSystem.getInputDescriptor("input4", ime -> 1f, new IntegerSerde());

    IMETransformingOutputDescriptor<String> output1 = imeTransformingSystem.getOutputDescriptor("output1");
    IMETransformingOutputDescriptor<Integer> output2 = imeTransformingSystem.getOutputDescriptor("output2", new IntegerSerde());

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
    IMETransformingSystemDescriptor imeTransformingSystem =
        new IMETransformingSystemDescriptor("imeTransformingSystem");

    Map<String, String> generatedConfigs = imeTransformingSystem.toConfig();
    assertEquals("org.apache.samza.IMETransformingSystemFactory",
        generatedConfigs.get("systems.imeTransformingSystem.samza.factory"));
  }

  @Test
  public void testISDObjectsWithOverrides() {
    IMETransformingSystemDescriptor imeTransformingSystem =
        new IMETransformingSystemDescriptor("imeTransformingSystem");

    InputTransformer<IncomingMessageEnvelope> transformer = m -> m;
    IntegerSerde streamSerde = new IntegerSerde();

    IMETransformingInputDescriptor<IncomingMessageEnvelope> overridingISD =
        imeTransformingSystem.getInputDescriptor("input-stream", transformer, streamSerde);

    assertEquals(streamSerde, overridingISD.getSerde());
    assertEquals(transformer, overridingISD.getTransformer().get());
  }

  @Test
  public void testISDObjectsWithDefaults() {
    IMETransformingSystemDescriptor imeTransformingSystem =
        new IMETransformingSystemDescriptor("imeTransformingSystem");

    IMETransformingInputDescriptor<Long> defaultISD =
        imeTransformingSystem.getInputDescriptor("input-stream");

    assertEquals(imeTransformingSystem.getSerde(), defaultISD.getSerde());
    assertEquals(imeTransformingSystem.getTransformer(), defaultISD.getTransformer().get());
  }
}
