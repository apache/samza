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
import org.apache.samza.operators.descriptors.expanding.GraphExpandingInputDescriptor;
import org.apache.samza.operators.descriptors.expanding.GraphExpandingOutputDescriptor;
import org.apache.samza.operators.descriptors.expanding.GraphExpandingSystemDescriptor;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestGraphExpandingDescriptors {
  public void testAPIUsage() {
    // does not assert anything, but acts as a compile-time check on expected descriptor type parameters
    // and validates that the method calls can be chained.
    GraphExpandingSystemDescriptor expandingSystem = new GraphExpandingSystemDescriptor("expandingSystem");

    GraphExpandingInputDescriptor<Long> input1 = expandingSystem.getInputDescriptor("input1");
    GraphExpandingInputDescriptor<Long> input2 = expandingSystem.getInputDescriptor("input2", new IntegerSerde());
    GraphExpandingInputDescriptor<Long> input3 = expandingSystem.getInputDescriptor("input3", ime -> 1f);
    GraphExpandingInputDescriptor<Long> input4 = expandingSystem.getInputDescriptor("input4", ime -> 1f, new IntegerSerde());

    GraphExpandingOutputDescriptor<String> output1 = expandingSystem.getOutputDescriptor("output1");
    GraphExpandingOutputDescriptor<Integer> output2 = expandingSystem.getOutputDescriptor("output2", new IntegerSerde());

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
    GraphExpandingSystemDescriptor expandingSystem = new GraphExpandingSystemDescriptor("expandingSystem");

    Map<String, String> generatedConfigs = expandingSystem.toConfig();
    assertEquals("org.apache.samza.GraphExpandingSystemFactory",
        generatedConfigs.get("systems.expandingSystem.samza.factory"));
  }

  @Test
  public void testISDObjectsWithOverrides() {
    GraphExpandingSystemDescriptor expandingSystem = new GraphExpandingSystemDescriptor("expandingSystem");

    InputTransformer<IncomingMessageEnvelope> transformer = m -> m;
    IntegerSerde streamSerde = new IntegerSerde();

    GraphExpandingInputDescriptor<Long> expandingISD =
        expandingSystem.getInputDescriptor("input-stream", transformer, streamSerde);

    assertEquals(streamSerde, expandingISD.getSerde());
    assertEquals(transformer, expandingISD.getTransformer().get());
  }

  @Test
  public void testISDObjectsWithDefaults() {
    GraphExpandingSystemDescriptor expandingSystem = new GraphExpandingSystemDescriptor("expandingSystem");
    GraphExpandingInputDescriptor<Long> defaultISD = expandingSystem.getInputDescriptor("input-stream");

    assertEquals(expandingSystem.getSystemSerde().get(), defaultISD.getSerde());
    assertEquals(expandingSystem.getTransformer(), defaultISD.getTransformer().get());
  }
}
