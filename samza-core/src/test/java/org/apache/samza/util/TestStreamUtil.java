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
package org.apache.samza.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.system.StreamSpec;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class TestStreamUtil {
  private static final String STREAM_ID = "t3st-Stream_Id";
  private static final String STREAM_ID_INVALID = "test#Str3amId!";

  private static final String TEST_PHYSICAL_NAME = "t3st-Physical_Name";
  private static final String TEST_PHYSICAL_NAME2 = "testPhysicalName2";
  private static final String TEST_PHYSICAL_NAME_SPECIAL_CHARS = "test://Physical.Name?";

  private static final String TEST_SYSTEM = "t3st-System_Name";
  private static final String TEST_SYSTEM_INVALID = "test:System!Name@";

  private static final String TEST_DEFAULT_SYSTEM = "testDefaultSystemName";


  // The physical name should be pulled from the StreamConfig.PHYSICAL_NAME property value.
  @Test
  public void testGetStreamWithPhysicalNameInConfig() {
    Config config = buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME,
        StreamConfig.SYSTEM(), TEST_SYSTEM);

    StreamSpec spec = StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));

    assertEquals(TEST_PHYSICAL_NAME, spec.getPhysicalName());
  }

  // The streamId should be used as the physicalName when the physical name is not specified.
  // NOTE: its either this, set to null, or exception. This seems better for backward compatibility and API brevity.
  @Test
  public void testGetStreamWithoutPhysicalNameInConfig() {
    Config config = buildStreamConfig(STREAM_ID,
        StreamConfig.SYSTEM(), TEST_SYSTEM);

    StreamSpec spec = StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));

    assertEquals(STREAM_ID, spec.getPhysicalName());
  }

  // If the system is specified at the stream scope, use it
  @Test
  public void testGetStreamWithSystemAtStreamScopeInConfig() {
    Config config = buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME,
        StreamConfig.SYSTEM(), TEST_SYSTEM);

    StreamSpec spec = StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));

    assertEquals(TEST_SYSTEM, spec.getSystemName());
  }

  // If system isn't specified at stream scope, use the default system
  @Test
  public void testGetStreamWithSystemAtDefaultScopeInConfig() {
    Config config = addConfigs(buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME),
        JobConfig.JOB_DEFAULT_SYSTEM(), TEST_DEFAULT_SYSTEM);

    StreamSpec spec = StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));

    assertEquals(TEST_DEFAULT_SYSTEM, spec.getSystemName());
  }

  // Stream scope should override default scope
  @Test
  public void testGetStreamWithSystemAtBothScopesInConfig() {
    Config config = addConfigs(buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME,
        StreamConfig.SYSTEM(), TEST_SYSTEM),
        JobConfig.JOB_DEFAULT_SYSTEM(), TEST_DEFAULT_SYSTEM);

    StreamSpec spec = StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));

    assertEquals(TEST_SYSTEM, spec.getSystemName());
  }

  // System is required. Throw if it cannot be determined.
  @Test(expected = IllegalArgumentException.class)
  public void testGetStreamWithOutSystemInConfig() {
    Config config = buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME);

    StreamSpec spec = StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));

    assertEquals(TEST_SYSTEM, spec.getSystemName());
  }

  // The properties in the config "streams.{streamId}.*" should be passed through to the spec.
  @Test
  public void testGetStreamPropertiesPassthrough() {
    Config config = buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME,
        StreamConfig.SYSTEM(), TEST_SYSTEM,
        "systemProperty1", "systemValue1",
        "systemProperty2", "systemValue2",
        "systemProperty3", "systemValue3");

    StreamSpec spec = StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));

    Map<String, String> properties = spec.getConfig();
    assertEquals(3, properties.size());
    assertEquals("systemValue1", properties.get("systemProperty1"));
    assertEquals("systemValue2", properties.get("systemProperty2"));
    assertEquals("systemValue3", properties.get("systemProperty3"));
    assertEquals("systemValue1", spec.get("systemProperty1"));
    assertEquals("systemValue2", spec.get("systemProperty2"));
    assertEquals("systemValue3", spec.get("systemProperty3"));
  }

  // The samza properties (which are invalid for the underlying system) should be filtered out.
  @Test
  public void testGetStreamSamzaPropertiesOmitted() {
    Config config = buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME,
        StreamConfig.SYSTEM(), TEST_SYSTEM,
        "systemProperty1", "systemValue1",
        "systemProperty2", "systemValue2",
        "systemProperty3", "systemValue3");

    StreamSpec spec = StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));

    Map<String, String> properties = spec.getConfig();
    assertEquals(3, properties.size());
    assertNull(properties.get(String.format(StreamConfig.PHYSICAL_NAME_FOR_STREAM_ID(), STREAM_ID)));
    assertNull(properties.get(String.format(StreamConfig.SYSTEM_FOR_STREAM_ID(), STREAM_ID)));
    assertNull(spec.get(String.format(StreamConfig.PHYSICAL_NAME_FOR_STREAM_ID(), STREAM_ID)));
    assertNull(spec.get(String.format(StreamConfig.SYSTEM_FOR_STREAM_ID(), STREAM_ID)));
  }

  @Test
  public void testStreamConfigOverrides() {
    final String sysStreamPrefix = String.format("systems.%s.streams.%s.", TEST_SYSTEM, TEST_PHYSICAL_NAME);
    Config config = addConfigs(buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME,
        StreamConfig.SYSTEM(), TEST_SYSTEM,
        "systemProperty1", "systemValue1",
        "systemProperty2", "systemValue2",
        "systemProperty3", "systemValue3"),
        sysStreamPrefix + "systemProperty4", "systemValue4",
        sysStreamPrefix + "systemProperty2", "systemValue8");

    StreamSpec spec = StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));

    Map<String, String> properties = spec.getConfig();
    assertEquals(4, properties.size());
    assertEquals("systemValue4", properties.get("systemProperty4"));
    assertEquals("systemValue2", properties.get("systemProperty2"));
  }

  // Verify that we use a default specified with systems.x.default.stream.*, if specified
  @Test
  public void testStreamConfigOverridesWithSystemDefaults() {
    Config config = addConfigs(buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME,
        StreamConfig.SYSTEM(), TEST_SYSTEM,
        "segment.bytes", "5309"),
        String.format("systems.%s.default.stream.replication.factor", TEST_SYSTEM), "4", // System default property
        String.format("systems.%s.default.stream.segment.bytest", TEST_SYSTEM), "867"
    );

    StreamSpec spec = StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));

    Map<String, String> properties = spec.getConfig();
    assertEquals(3, properties.size());
    assertEquals("4", properties.get("replication.factor")); // Uses system default
    assertEquals("5309", properties.get("segment.bytes")); // Overrides system default
  }

  // When the physicalName argument is passed explicitly it should be used, regardless of whether it is also in the config
  @Test
  public void testGetStreamPhysicalNameArgSimple() {
    Config config = buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME2, // This should be ignored because of the explicit arg
        StreamConfig.SYSTEM(), TEST_SYSTEM);

    StreamSpec spec = StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));

    assertEquals(STREAM_ID, spec.getId());
    assertEquals(TEST_PHYSICAL_NAME2, spec.getPhysicalName());
    assertEquals(TEST_SYSTEM, spec.getSystemName());
  }

  // Special characters are allowed for the physical name
  @Test
  public void testGetStreamPhysicalNameArgSpecialCharacters() {
    Config config = buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME_SPECIAL_CHARS,
        StreamConfig.SYSTEM(), TEST_SYSTEM);

    StreamSpec spec = StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));
    assertEquals(TEST_PHYSICAL_NAME_SPECIAL_CHARS, spec.getPhysicalName());
  }

  // Null is allowed for the physical name
  @Test
  public void testGetStreamPhysicalNameArgNull() {
    Config config = buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), null,
        StreamConfig.SYSTEM(), TEST_SYSTEM);

    StreamSpec spec = StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));
    assertNull(spec.getPhysicalName());
  }

  // When the system name is provided explicitly, it should be used, regardless of whether it's also in the config
  @Test
  public void testGetStreamSystemNameArgValid() {
    Config config = buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME, // This should be ignored because of the explicit arg
        StreamConfig.SYSTEM(), TEST_SYSTEM);              // This too

    StreamSpec spec = StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));

    assertEquals(STREAM_ID, spec.getId());
    assertEquals(TEST_PHYSICAL_NAME, spec.getPhysicalName());
    assertEquals(TEST_SYSTEM, spec.getSystemName());
  }

  // Special characters are NOT allowed for system name, because it's used as an identifier in the config.
  @Test(expected = IllegalArgumentException.class)
  public void testGetStreamSystemNameArgInvalid() {
    Config config = buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME,
        StreamConfig.SYSTEM(), TEST_SYSTEM_INVALID);

    StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));
  }

  // Empty strings are NOT allowed for system name, because it's used as an identifier in the config.
  @Test(expected = IllegalArgumentException.class)
  public void testGetStreamSystemNameArgEmpty() {
    Config config = buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME,
        StreamConfig.SYSTEM(), "");

    StreamSpec spec = StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));
  }

  // Null is not allowed IllegalArgumentException system name.
  @Test(expected = IllegalArgumentException.class)
  public void testGetStreamSystemNameArgNull() {
    Config config = buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME,
        StreamConfig.SYSTEM(), null);

    StreamUtil.getStreamSpec(STREAM_ID, new StreamConfig(config));
  }

  // Special characters are NOT allowed for streamId, because it's used as an identifier in the config.
  @Test(expected = IllegalArgumentException.class)
  public void testGetStreamStreamIdInvalid() {
    Config config = buildStreamConfig(STREAM_ID_INVALID,
        StreamConfig.SYSTEM(), TEST_SYSTEM);

    StreamUtil.getStreamSpec(STREAM_ID_INVALID, new StreamConfig(config));
  }

  // Empty strings are NOT allowed for streamId, because it's used as an identifier in the config.
  @Test(expected = IllegalArgumentException.class)
  public void testGetStreamStreamIdEmpty() {
    Config config = buildStreamConfig("",
        StreamConfig.SYSTEM(), TEST_SYSTEM);

    StreamUtil.getStreamSpec("", new StreamConfig(config));
  }

  // Null is not allowed for streamId.
  @Test(expected = IllegalArgumentException.class)
  public void testGetStreamStreamIdNull() {
    Config config = buildStreamConfig(null,
        StreamConfig.SYSTEM(), TEST_SYSTEM);

    StreamUtil.getStreamSpec(null, new StreamConfig(config));
  }


  // Helper methods

  private Config buildStreamConfig(String streamId, String... kvs) {
    // inject streams.x. into each key
    for (int i = 0; i < kvs.length - 1; i += 2) {
      kvs[i] = String.format(StreamConfig.STREAM_ID_PREFIX(), streamId) + kvs[i];
    }
    return buildConfig(kvs);
  }

  private Config buildConfig(String... kvs) {
    if (kvs.length % 2 != 0) {
      throw new IllegalArgumentException("There must be parity between the keys and values");
    }

    Map<String, String> configMap = new HashMap<>();
    for (int i = 0; i < kvs.length - 1; i += 2) {
      configMap.put(kvs[i], kvs[i + 1]);
    }
    return new MapConfig(configMap);
  }

  private Config addConfigs(Config original, String... kvs) {
    Map<String, String> result = new HashMap<>();
    result.putAll(original);
    result.putAll(buildConfig(kvs));
    return new MapConfig(result);
  }
}
