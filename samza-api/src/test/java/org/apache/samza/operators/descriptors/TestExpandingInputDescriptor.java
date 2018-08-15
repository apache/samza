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
import org.apache.samza.operators.descriptors.expanding.MockExpandingInputDescriptor;
import org.apache.samza.operators.descriptors.expanding.MockExpandingOutputDescriptor;
import org.apache.samza.operators.descriptors.expanding.MockExpandingSystemDescriptor;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.system.SystemStreamMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestExpandingInputDescriptor {
  public void testAPIUsage() {
    // does not assert anything, but acts as a compile-time check on expected descriptor type parameters
    // and validates that the method calls can be chained.
    MockExpandingSystemDescriptor expandingSystem = new MockExpandingSystemDescriptor("expandingSystem");
    MockExpandingInputDescriptor<Long> input1 = expandingSystem.getInputDescriptor("input1", new IntegerSerde());
    MockExpandingOutputDescriptor<Integer> output1 = expandingSystem.getOutputDescriptor("output1", new IntegerSerde());

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
  public void testISDObjectsWithOverrides() {
    MockExpandingSystemDescriptor expandingSystem = new MockExpandingSystemDescriptor("expandingSystem");
    IntegerSerde streamSerde = new IntegerSerde();
    MockExpandingInputDescriptor<Long> expandingISD = expandingSystem.getInputDescriptor("input-stream", streamSerde);

    assertEquals(streamSerde, expandingISD.getSerde());
    assertEquals(expandingSystem.getTransformer().get(), expandingISD.getTransformer().get());
  }
}
