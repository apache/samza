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
package org.apache.samza.config;

import java.util.List;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.SamzaException;
import org.apache.samza.serializers.ByteBufferSerdeFactory;
import org.apache.samza.serializers.ByteSerdeFactory;
import org.apache.samza.serializers.DoubleSerdeFactory;
import org.apache.samza.serializers.IntegerSerdeFactory;
import org.apache.samza.serializers.JsonSerdeFactory;
import org.apache.samza.serializers.LongSerdeFactory;
import org.apache.samza.serializers.SerializableSerdeFactory;
import org.apache.samza.serializers.StringSerdeFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class TestSerializerConfig {
  private static final String SERDE_NAME = "mySerdeName";

  @Test
  public void testGetSerdeFactoryName() {
    assertEquals(ByteSerdeFactory.class.getCanonicalName(), SerializerConfig.getSerdeFactoryName("byte"));
    assertEquals(ByteBufferSerdeFactory.class.getCanonicalName(),
        SerializerConfig.getSerdeFactoryName("bytebuffer"));
    assertEquals(IntegerSerdeFactory.class.getCanonicalName(), SerializerConfig.getSerdeFactoryName("integer"));
    assertEquals(JsonSerdeFactory.class.getCanonicalName(), SerializerConfig.getSerdeFactoryName("json"));
    assertEquals(LongSerdeFactory.class.getCanonicalName(), SerializerConfig.getSerdeFactoryName("long"));
    assertEquals(SerializableSerdeFactory.class.getCanonicalName(),
        SerializerConfig.getSerdeFactoryName("serializable"));
    assertEquals(StringSerdeFactory.class.getCanonicalName(), SerializerConfig.getSerdeFactoryName("string"));
    assertEquals(DoubleSerdeFactory.class.getCanonicalName(), SerializerConfig.getSerdeFactoryName("double"));
  }

  @Test(expected = SamzaException.class)
  public void testGetSerdeFactoryNameUnknown() {
    SerializerConfig.getSerdeFactoryName("otherName");
  }

  @Test
  public void testGetSerdeClass() {
    String serdeClassName = "my.class.serde.name";
    Config config = new MapConfig(
        ImmutableMap.of(String.format(SerializerConfig.SERDE_FACTORY_CLASS, SERDE_NAME), serdeClassName));
    assertEquals(serdeClassName, new SerializerConfig(config).getSerdeClass(SERDE_NAME).get());
    assertFalse(new SerializerConfig(config).getSerdeClass("otherSerdeName").isPresent());
  }

  @Test
  public void testGetSerdeNames() {
    String otherSerdeName = "otherSerdeName";
    String serdeClassName = "my.class.serde.name";
    Config config = new MapConfig(
        ImmutableMap.of(String.format(SerializerConfig.SERDE_FACTORY_CLASS, SERDE_NAME), serdeClassName,
            String.format(SerializerConfig.SERDE_FACTORY_CLASS, otherSerdeName), serdeClassName));
    List<String> serdeNames = new SerializerConfig(config).getSerdeNames();
    ImmutableSet<String> expectedSerdeNames = ImmutableSet.of(SERDE_NAME, otherSerdeName);
    assertEquals(expectedSerdeNames.size(), serdeNames.size()); // ensures no duplicates
    // can't check ordering in a stable way since values come from the key set of a map, so just check entries
    assertEquals(expectedSerdeNames, ImmutableSet.copyOf(serdeNames));
  }
}
