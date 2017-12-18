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
package org.apache.samza.system.kafka;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Properties;
import org.apache.samza.system.StreamSpec;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * See also the general StreamSpec tests in {@link org.apache.samza.runtime.TestAbstractApplicationRunner}
 */
public class TestKafkaStreamSpec {

  @Test
  public void testUnsupportedConfigStrippedFromProperties() {
    StreamSpec original = new StreamSpec("dummyId","dummyPhysicalName", "dummySystemName", false, ImmutableMap.of("segment.bytes", "4", "replication.factor", "7"));

    // First verify the original
    assertEquals("7", original.get("replication.factor"));
    assertEquals("4", original.get("segment.bytes"));

    Map<String, String> config = original.getConfig();
    assertEquals("7", config.get("replication.factor"));
    assertEquals("4", config.get("segment.bytes"));


    // Now verify the Kafka spec
    KafkaStreamSpec spec = KafkaStreamSpec.fromSpec(original);
    assertNull(spec.get("replication.factor"));
    assertEquals("4", spec.get("segment.bytes"));

    Properties kafkaProperties = spec.getProperties();
    Map<String, String> kafkaConfig = spec.getConfig();
    assertNull(kafkaProperties.get("replication.factor"));
    assertEquals("4", kafkaProperties.get("segment.bytes"));

    assertNull(kafkaConfig.get("replication.factor"));
    assertEquals("4", kafkaConfig.get("segment.bytes"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPartitionCount() {
    new KafkaStreamSpec("dummyId","dummyPhysicalName", "dummySystemName", 0);
  }
}
