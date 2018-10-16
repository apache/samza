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

import java.util.Collections;
import org.apache.samza.system.descriptors.examples.transforming.ExampleTransformingInputDescriptor;
import org.apache.samza.system.descriptors.examples.transforming.ExampleTransformingOutputDescriptor;
import org.apache.samza.system.descriptors.examples.transforming.ExampleTransformingSystemDescriptor;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.system.SystemStreamMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestTransformingInputDescriptor {
  @Test
  public void testAPIUsage() {
    // does not assert anything, but acts as a compile-time check on expected descriptor type parameters
    // and validates that the method calls can be chained.
    ExampleTransformingSystemDescriptor imeTransformingSystem =
        new ExampleTransformingSystemDescriptor("imeTransformingSystem")
            .withSystemConfigs(Collections.emptyMap());
    ExampleTransformingInputDescriptor<Long> input1 = imeTransformingSystem.getInputDescriptor("input1", new IntegerSerde());
    ExampleTransformingOutputDescriptor<Integer> output1 = imeTransformingSystem.getOutputDescriptor("output1", new IntegerSerde());

    input1
        .shouldBootstrap()
        .withOffsetDefault(SystemStreamMetadata.OffsetType.NEWEST)
        .withPriority(1)
        .shouldResetOffset()
        .withStreamConfigs(Collections.emptyMap());

    output1
        .withStreamConfigs(Collections.emptyMap());
  }

  @Test
  public void testISDObjectsWithOverrides() {
    ExampleTransformingSystemDescriptor imeTransformingSystem =
        new ExampleTransformingSystemDescriptor("imeTransformingSystem");
    IntegerSerde streamSerde = new IntegerSerde();
    ExampleTransformingInputDescriptor<Long> overridingISD =
        imeTransformingSystem.getInputDescriptor("input-stream", streamSerde);

    assertEquals(streamSerde, overridingISD.getSerde());
    assertEquals(imeTransformingSystem.getTransformer().get(), overridingISD.getTransformer().get());
  }
}
