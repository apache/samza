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
import org.apache.samza.system.SystemStreamMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestGenericSystemDescriptor {
  @Test
  public void testSDConfigs() {
    GenericSystemDescriptor mySystem =
        new GenericSystemDescriptor("input-system", "factory.class.name")
            .withSystemConfigs(ImmutableMap.of("custom-config-key", "custom-config-value"))
            .withDefaultStreamConfigs(ImmutableMap.of("custom-stream-config-key", "custom-stream-config-value"))
            .withDefaultStreamOffsetDefault(SystemStreamMetadata.OffsetType.UPCOMING);

    Map<String, String> generatedConfigs = mySystem.toConfig(Collections.emptyMap());
    Map<String, String> expectedConfigs = ImmutableMap.of(
        "systems.input-system.samza.factory", "factory.class.name",
        "systems.input-system.custom-config-key", "custom-config-value",
        "systems.input-system.default.stream.custom-stream-config-key", "custom-stream-config-value",
        "systems.input-system.default.stream.samza.offset.default", "upcoming"
    );
    assertEquals(expectedConfigs, generatedConfigs);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetInputDescriptorWithNullSerde() {
    GenericSystemDescriptor mySystem = new GenericSystemDescriptor("input-system", "factory.class.name");
    mySystem.getInputDescriptor("streamId", null); // should throw an exception
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSystemDescriptorWithNullSystemName() {
    new GenericSystemDescriptor(null, "factory.class.name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSystemDescriptorWithEmptySystemName() {
    new GenericSystemDescriptor(" ", "factory.class.name");
  }
}
