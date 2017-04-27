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

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.system.SystemStream;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestStreamConfig {
  private static final String SYSTEM_STREAM_PATTERN = "systems.%s.streams.%s.";
  private static final String STREAM_ID_PATTERN = "streams.%s.";

  private static final String STREAM1_SYSTEM = "Sys1";
  private static final String STREAM1_PHYSICAL_NAME = "Str1";
  private static final String STREAM1_STREAM_ID = "streamId1";
  private static final SystemStream SYSTEM_STREAM_1 = new SystemStream(STREAM1_SYSTEM, STREAM1_PHYSICAL_NAME);

  private static final String STREAM2_SYSTEM = "Sys2";
  private static final String STREAM2_PHYSICAL_NAME = "Str2";
  private static final String STREAM2_STREAM_ID = "streamId2";
  private static final SystemStream SYSTEM_STREAM_2 = new SystemStream(STREAM2_SYSTEM, STREAM2_PHYSICAL_NAME);

  private static final String STREAM3_SYSTEM = "Sys3";
  private static final String STREAM3_PHYSICAL_NAME = "Str3";
  private static final String STREAM3_STREAM_ID = "streamId3";
  private static final SystemStream SYSTEM_STREAM_3 = new SystemStream(STREAM3_SYSTEM, STREAM3_PHYSICAL_NAME);

  private static final String SYSTEM_DEFAULT_STREAM_PATTERN = "systems.%s.default.stream.";


  @Test(expected = IllegalArgumentException.class)
  public void testGetSamzaPropertyThrowsIfInvalidPropertyName() {
    StreamConfig config = buildConfig("key1", "value1", "key2", "value2");
    config.getSamzaProperty(SYSTEM_STREAM_1, "key1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSamzaPropertyWithDefaultThrowsIfInvalidPropertyName() {
    StreamConfig config = buildConfig("key1", "value1", "key2", "value2");
    config.getSamzaProperty(SYSTEM_STREAM_1, "key1", "default");
  }

  // 00
  @Test
  public void testGetSamzaPropertyDoesNotExist() {
    StreamConfig config = buildConfig(buildProp(SYSTEM_STREAM_1, "samza.key1"), "value1", "key2", "value2");
    assertNull(config.getSamzaProperty(SYSTEM_STREAM_1, "samza.keyNonExistent"));
  }

  // 01
  @Test
  public void testGetSamzaPropertyFromSystemStream() {
    StreamConfig config = buildConfig(buildProp(SYSTEM_STREAM_1, "samza.key1"), "value1", "key2", "value2");
    assertEquals("value1", config.getSamzaProperty(SYSTEM_STREAM_1, "samza.key1"));
  }

  // 10
  @Test
  public void testGetSamzaPropertyFromStreamId() {
    StreamConfig config = buildConfig("key1", "value1",
        buildProp(STREAM1_STREAM_ID, "samza.key2"), "value2",
        buildProp(STREAM1_STREAM_ID, StreamConfig.SYSTEM()), STREAM1_SYSTEM,
        buildProp(STREAM1_STREAM_ID, StreamConfig.PHYSICAL_NAME()), STREAM1_PHYSICAL_NAME);
    assertEquals("value2", config.getSamzaProperty(SYSTEM_STREAM_1, "samza.key2"));
  }

  // 11
  @Test
  public void testGetSamzaPropertyFromSystemStreamAndStreamId() {
    StreamConfig config = buildConfig("key1", "value1",
        buildProp(SYSTEM_STREAM_1, "samza.key2"), "value2",
        buildProp(STREAM1_STREAM_ID, "samza.key2"), "value2OVERRIDE",
        buildProp(STREAM1_STREAM_ID, StreamConfig.SYSTEM()), STREAM1_SYSTEM,
        buildProp(STREAM1_STREAM_ID, StreamConfig.PHYSICAL_NAME()), STREAM1_PHYSICAL_NAME);
    assertEquals("value2OVERRIDE", config.getSamzaProperty(SYSTEM_STREAM_1, "samza.key2"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testContainsSamzaPropertyThrowsIfInvalidPropertyName() {
    StreamConfig config = buildConfig("key1", "value1", "key2", "value2");
    config.containsSamzaProperty(new SystemStream("SysX", "StrX"), "key1");
  }

  // 00
  @Test
  public void testContainsSamzaPropertyDoesNotExist() {
    StreamConfig config = buildConfig(buildProp(SYSTEM_STREAM_1, "samza.key1"), "value1", "key2", "value2");
    assertFalse(config.containsSamzaProperty(SYSTEM_STREAM_1, "samza.keyNonExistent"));
  }

  // 01
  @Test
  public void testContainsSamzaPropertyFromSystemStream() {
    StreamConfig config = buildConfig(buildProp(SYSTEM_STREAM_1, "samza.key1"), "value1", "key2", "value2");
    assertTrue(config.containsSamzaProperty(SYSTEM_STREAM_1, "samza.key1"));
  }

  // 10
  @Test
  public void testContainsSamzaPropertyFromStreamId() {
    StreamConfig config = buildConfig("key1", "value1",
        buildProp(STREAM1_STREAM_ID, "samza.key2"), "value2",
        buildProp(STREAM1_STREAM_ID, StreamConfig.SYSTEM()), STREAM1_SYSTEM,
        buildProp(STREAM1_STREAM_ID, StreamConfig.PHYSICAL_NAME()), STREAM1_PHYSICAL_NAME);
    assertTrue(config.containsSamzaProperty(SYSTEM_STREAM_1, "samza.key2"));
  }

  // 11
  @Test
  public void testContainsSamzaPropertyFromSystemStreamAndStreamId() {
    StreamConfig config = buildConfig("key1", "value1",
        buildProp(SYSTEM_STREAM_1, "samza.key2"), "value2",
        buildProp(STREAM1_STREAM_ID, "samza.key2"), "value2OVERRIDE",
        buildProp(STREAM1_STREAM_ID, StreamConfig.SYSTEM()), STREAM1_SYSTEM,
        buildProp(STREAM1_STREAM_ID, StreamConfig.PHYSICAL_NAME()), STREAM1_PHYSICAL_NAME);
    assertTrue(config.containsSamzaProperty(SYSTEM_STREAM_1, "samza.key2"));
  }

  // 00
  @Test
  public void testGetSerdeStreamsDoesNotExist() {
    StreamConfig config = buildConfig(buildProp(SYSTEM_STREAM_1, "samza.key1"), "value1", "key2", "value2");
    assertTrue(config.getSerdeStreams(STREAM1_SYSTEM).isEmpty());
  }

  // 01
  @Test
  public void testGetSerdeStreamsFromSystemStream() {
    StreamConfig config = buildConfig(buildProp(SYSTEM_STREAM_1, StreamConfig.KEY_SERDE()), "value1",
        buildProp(SYSTEM_STREAM_2, StreamConfig.MSG_SERDE()), "value2",
        "key3", "value3");
    assertEquals(1, config.getSerdeStreams(STREAM1_SYSTEM).size());
    assertEquals(1, config.getSerdeStreams(STREAM2_SYSTEM).size());
    assertEquals("value1", config.getStreamKeySerde(SYSTEM_STREAM_1).get());
    assertEquals("value2", config.getStreamMsgSerde(SYSTEM_STREAM_2).get());
  }

  // 10
  @Test
  public void testGetSerdeStreamsFromStreamId() {
    StreamConfig config = buildConfig(
        buildProp(STREAM1_STREAM_ID, StreamConfig.SYSTEM()), STREAM1_SYSTEM,
        buildProp(STREAM1_STREAM_ID, StreamConfig.PHYSICAL_NAME()), STREAM1_PHYSICAL_NAME,
        buildProp(STREAM1_STREAM_ID, StreamConfig.KEY_SERDE()), "value1",

        buildProp(STREAM2_STREAM_ID, StreamConfig.SYSTEM()), STREAM2_SYSTEM,
        buildProp(STREAM2_STREAM_ID, StreamConfig.PHYSICAL_NAME()), STREAM2_PHYSICAL_NAME,
        buildProp(STREAM2_STREAM_ID, StreamConfig.MSG_SERDE()), "value2",
        "key3", "value3");
    assertEquals(1, config.getSerdeStreams(STREAM1_SYSTEM).size());
    assertEquals(1, config.getSerdeStreams(STREAM2_SYSTEM).size());
    assertEquals("value1", config.getStreamKeySerde(SYSTEM_STREAM_1).get());
    assertEquals("value2", config.getStreamMsgSerde(SYSTEM_STREAM_2).get());
  }

  // 11
  @Test
  public void testGetSerdeStreamsFromSystemStreamAndStreamId() {
    StreamConfig config = buildConfig(buildProp(SYSTEM_STREAM_1, StreamConfig.KEY_SERDE()), "value1",
        buildProp(STREAM1_STREAM_ID, StreamConfig.SYSTEM()), STREAM1_SYSTEM,
        buildProp(STREAM1_STREAM_ID, StreamConfig.PHYSICAL_NAME()), STREAM1_PHYSICAL_NAME,
        buildProp(STREAM1_STREAM_ID, StreamConfig.KEY_SERDE()), "value1OVERRIDE",
        buildProp(STREAM2_STREAM_ID, StreamConfig.SYSTEM()), STREAM2_SYSTEM,
        buildProp(STREAM2_STREAM_ID, StreamConfig.PHYSICAL_NAME()), STREAM2_PHYSICAL_NAME,
        buildProp(STREAM2_STREAM_ID, StreamConfig.MSG_SERDE()), "value2",
        "key3", "value3");

    assertEquals(1, config.getSerdeStreams(STREAM1_SYSTEM).size());
    assertEquals(1, config.getSerdeStreams(STREAM2_SYSTEM).size());
    assertEquals("value1OVERRIDE", config.getStreamKeySerde(SYSTEM_STREAM_1).get());
    assertEquals("value2", config.getStreamMsgSerde(SYSTEM_STREAM_2).get());
  }

  @Test
  public void testStreamPropertyDefaults() {
    final String nonSamzaProperty = "replication.factor";
    StreamConfig config = buildConfig(
        buildSystemDefaultProp(STREAM1_SYSTEM, nonSamzaProperty), "1",
        buildSystemDefaultProp(STREAM1_SYSTEM, StreamConfig.KEY_SERDE()), "value1",
        buildSystemDefaultProp(STREAM1_SYSTEM, StreamConfig.CONSUMER_OFFSET_DEFAULT()), "newest",
        buildProp(SYSTEM_STREAM_1, "dummyStreamProperty"), "dummyValue",
        buildProp(STREAM1_STREAM_ID, StreamConfig.SYSTEM()), STREAM1_SYSTEM,
        buildProp(STREAM1_STREAM_ID, StreamConfig.PHYSICAL_NAME()), STREAM1_PHYSICAL_NAME,
        buildSystemDefaultProp(STREAM2_SYSTEM, nonSamzaProperty), "2",
        buildProp(STREAM2_STREAM_ID, StreamConfig.SYSTEM()), STREAM2_SYSTEM,
        buildProp(STREAM2_STREAM_ID, StreamConfig.PHYSICAL_NAME()), STREAM2_PHYSICAL_NAME,
        buildProp(STREAM2_STREAM_ID, nonSamzaProperty), "3",
        buildSystemDefaultProp(STREAM3_SYSTEM, nonSamzaProperty), "4",
        buildProp(STREAM3_STREAM_ID, StreamConfig.SYSTEM()), STREAM3_SYSTEM,
        buildProp(STREAM3_STREAM_ID, StreamConfig.PHYSICAL_NAME()), STREAM3_PHYSICAL_NAME,
        buildProp(SYSTEM_STREAM_3, nonSamzaProperty), "5",
        "key3", "value3");



    // Ensure that we can set legacy system properties via the new system wide default
    assertEquals("value1", config.getStreamKeySerde(SYSTEM_STREAM_1).get());
    assertEquals(1, config.getSerdeStreams(STREAM1_SYSTEM).size());
    assertEquals("newest", config.getDefaultStreamOffset(SYSTEM_STREAM_1).get());

    // Property set via systems.x.default.stream.* only
    assertEquals("1", config.getStreamProperties(STREAM1_STREAM_ID).get(nonSamzaProperty));

    // Property set via systems.x.default.stream.* and streams.y.*
    assertEquals("3", config.getStreamProperties(STREAM2_STREAM_ID).get(nonSamzaProperty));

    // Property set via systems.x.default.stream.* and system.x.streams.z.*
    assertEquals("5", config.getStreamProperties(STREAM3_STREAM_ID).get(nonSamzaProperty));
  }


  private StreamConfig buildConfig(String... kvs) {
    if (kvs.length % 2 != 0) {
      throw new IllegalArgumentException("There must be parity between the keys and values");
    }

    Map<String, String> configMap = new HashMap<>();
    for (int i = 0; i < kvs.length - 1; i += 2) {
      configMap.put(kvs[i], kvs[i + 1]);
    }
    return new StreamConfig(new MapConfig(configMap));
  }

  private String buildProp(String streamId, String suffix) {
    return String.format(STREAM_ID_PATTERN, streamId) + suffix;
  }

  private String buildProp(SystemStream systemStream, String suffix) {
    return String.format(SYSTEM_STREAM_PATTERN, systemStream.getSystem(), systemStream.getStream()) + suffix;
  }

  private String buildSystemDefaultProp(String system, String suffix) {
    return String.format(SYSTEM_DEFAULT_STREAM_PATTERN, system) + suffix;
  }

  private Config addConfigs(Config original, String... kvs) {
    Map<String, String> result = new HashMap<>();
    result.putAll(original);
    result.putAll(buildConfig(kvs));
    return new MapConfig(result);
  }

}
