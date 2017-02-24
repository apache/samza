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
package org.apache.samza.system;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.operators.StreamGraphBuilder;
import org.junit.Test;

import static org.junit.Assert.*;
public class TestAbstractExecutionEnvironment {
  private static final String STREAM_ID = "t3st-Stream_Id";
  private static final String STREAM_ID_INVALID = "test#Str3amId!";

  private static final String TEST_PHYSICAL_NAME = "t3st-Physical_Name";
  private static final String TEST_PHYSICAL_NAME2 = "testPhysicalName2";
  private static final String TEST_PHYSICAL_NAME_SPECIAL_CHARS = "test://Physical.Name?";

  private static final String TEST_SYSTEM = "t3st-System_Name";
  private static final String TEST_SYSTEM2 = "testSystemName2";
  private static final String TEST_SYSTEM_INVALID = "test:System!Name@";

  private static final String TEST_DEFAULT_SYSTEM = "testDefaultSystemName";


  @Test(expected = NullPointerException.class)
  public void testConfigValidation() {
    new TestAbstractExecutionEnvironmentImpl(null);
  }

  // The physical name should be pulled from the StreamConfig.PHYSICAL_NAME property value.
  @Test
  public void testStreamFromConfigWithPhysicalNameInConfig() {
    Config config = buildStreamConfig(STREAM_ID,
                                      StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME,
                                      StreamConfig.SYSTEM(), TEST_SYSTEM);

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    StreamSpec spec = env.streamFromConfig(STREAM_ID);

    assertEquals(TEST_PHYSICAL_NAME, spec.getPhysicalName());
  }

  // The streamId should be used as the physicalName when the physical name is not specified.
  // NOTE: its either this, set to null, or exception. This seems better for backward compatibility and API brevity.
  @Test
  public void testStreamFromConfigWithoutPhysicalNameInConfig() {
    Config config = buildStreamConfig(STREAM_ID,
                                      StreamConfig.SYSTEM(), TEST_SYSTEM);

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    StreamSpec spec = env.streamFromConfig(STREAM_ID);

    assertEquals(STREAM_ID, spec.getPhysicalName());
  }

  // If the system is specified at the stream scope, use it
  @Test
  public void testStreamFromConfigWithSystemAtStreamScopeInConfig() {
    Config config = buildStreamConfig(STREAM_ID,
                                      StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME,
                                      StreamConfig.SYSTEM(), TEST_SYSTEM);

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    StreamSpec spec = env.streamFromConfig(STREAM_ID);

    assertEquals(TEST_SYSTEM, spec.getSystemName());
  }

  // If system isn't specified at stream scope, use the default system
  @Test
  public void testStreamFromConfigWithSystemAtDefaultScopeInConfig() {
    Config config = addConfigs(buildStreamConfig(STREAM_ID,
                                                  StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME),
                                JobConfig.JOB_DEFAULT_SYSTEM(), TEST_DEFAULT_SYSTEM);

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    StreamSpec spec = env.streamFromConfig(STREAM_ID);

    assertEquals(TEST_DEFAULT_SYSTEM, spec.getSystemName());
  }

  // Stream scope should override default scope
  @Test
  public void testStreamFromConfigWithSystemAtBothScopesInConfig() {
    Config config = addConfigs(buildStreamConfig(STREAM_ID,
                                                StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME,
                                                StreamConfig.SYSTEM(), TEST_SYSTEM),
                                JobConfig.JOB_DEFAULT_SYSTEM(), TEST_DEFAULT_SYSTEM);

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    StreamSpec spec = env.streamFromConfig(STREAM_ID);

    assertEquals(TEST_SYSTEM, spec.getSystemName());
  }

  // System is required. Throw if it cannot be determined.
  @Test(expected = Exception.class)
  public void testStreamFromConfigWithOutSystemInConfig() {
    Config config = buildStreamConfig(STREAM_ID,
                                      StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME);

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    StreamSpec spec = env.streamFromConfig(STREAM_ID);

    assertEquals(TEST_SYSTEM, spec.getSystemName());
  }

  // The properties in the config "streams.{streamId}.*" should be passed through to the spec.
  @Test
  public void testStreamFromConfigPropertiesPassthrough() {
    Config config = buildStreamConfig(STREAM_ID,
                                    StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME,
                                    StreamConfig.SYSTEM(), TEST_SYSTEM,
                                    "systemProperty1", "systemValue1",
                                    "systemProperty2", "systemValue2",
                                    "systemProperty3", "systemValue3");

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    StreamSpec spec = env.streamFromConfig(STREAM_ID);

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
  public void testStreamFromConfigSamzaPropertiesOmitted() {
    Config config = buildStreamConfig(STREAM_ID,
                              StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME,
                                    StreamConfig.SYSTEM(), TEST_SYSTEM,
                                    "systemProperty1", "systemValue1",
                                    "systemProperty2", "systemValue2",
                                    "systemProperty3", "systemValue3");

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    StreamSpec spec = env.streamFromConfig(STREAM_ID);

    Map<String, String> properties = spec.getConfig();
    assertEquals(3, properties.size());
    assertNull(properties.get(String.format(StreamConfig.PHYSICAL_NAME_FOR_STREAM_ID(), STREAM_ID)));
    assertNull(properties.get(String.format(StreamConfig.SYSTEM_FOR_STREAM_ID(), STREAM_ID)));
    assertNull(spec.get(String.format(StreamConfig.PHYSICAL_NAME_FOR_STREAM_ID(), STREAM_ID)));
    assertNull(spec.get(String.format(StreamConfig.SYSTEM_FOR_STREAM_ID(), STREAM_ID)));
  }

  // When the physicalName argument is passed explicitly it should be used, regardless of whether it is also in the config
  @Test
  public void testStreamFromConfigPhysicalNameArgSimple() {
    Config config = buildStreamConfig(STREAM_ID,
                                      StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME2, // This should be ignored because of the explicit arg
                                      StreamConfig.SYSTEM(), TEST_SYSTEM);

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    StreamSpec spec = env.streamFromConfig(STREAM_ID, TEST_PHYSICAL_NAME);

    assertEquals(STREAM_ID, spec.getId());
    assertEquals(TEST_PHYSICAL_NAME, spec.getPhysicalName());
    assertEquals(TEST_SYSTEM, spec.getSystemName());
  }

  // Special characters are allowed for the physical name
  @Test
  public void testStreamFromConfigPhysicalNameArgSpecialCharacters() {
    Config config = buildStreamConfig(STREAM_ID,
                                      StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME2,
                                      StreamConfig.SYSTEM(), TEST_SYSTEM);

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    StreamSpec spec = env.streamFromConfig(STREAM_ID, TEST_PHYSICAL_NAME_SPECIAL_CHARS);
    assertEquals(TEST_PHYSICAL_NAME_SPECIAL_CHARS, spec.getPhysicalName());
  }

  // Null is allowed for the physical name
  @Test
  public void testStreamFromConfigPhysicalNameArgNull() {
    Config config = buildStreamConfig(STREAM_ID,
                                      StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME2,
                                      StreamConfig.SYSTEM(), TEST_SYSTEM);

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    StreamSpec spec = env.streamFromConfig(STREAM_ID, null);
    assertNull(spec.getPhysicalName());
  }

  // When the system name is provided explicitly, it should be used, regardless of whether it's also in the config
  @Test
  public void testStreamFromConfigSystemNameArgValid() {
    Config config = buildStreamConfig(STREAM_ID,
                                      StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME2, // This should be ignored because of the explicit arg
                                      StreamConfig.SYSTEM(), TEST_SYSTEM2);              // This too

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    StreamSpec spec = env.streamFromConfig(STREAM_ID, TEST_PHYSICAL_NAME, TEST_SYSTEM);

    assertEquals(STREAM_ID, spec.getId());
    assertEquals(TEST_PHYSICAL_NAME, spec.getPhysicalName());
    assertEquals(TEST_SYSTEM, spec.getSystemName());
  }

  // Special characters are NOT allowed for system name, because it's used as an identifier in the config.
  @Test(expected = IllegalArgumentException.class)
  public void testStreamFromConfigSystemNameArgInvalid() {
    Config config = buildStreamConfig(STREAM_ID,
                                      StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME2,
                                      StreamConfig.SYSTEM(), TEST_SYSTEM2);

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    env.streamFromConfig(STREAM_ID, TEST_PHYSICAL_NAME, TEST_SYSTEM_INVALID);
  }

  // Empty strings are NOT allowed for system name, because it's used as an identifier in the config.
  @Test(expected = IllegalArgumentException.class)
  public void testStreamFromConfigSystemNameArgEmpty() {
    Config config = buildStreamConfig(STREAM_ID,
        StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME2,
        StreamConfig.SYSTEM(), TEST_SYSTEM2);

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    env.streamFromConfig(STREAM_ID, TEST_PHYSICAL_NAME, "");
  }

  // Null is not allowed for system name.
  @Test(expected = NullPointerException.class)
  public void testStreamFromConfigSystemNameArgNull() {
    Config config = buildStreamConfig(STREAM_ID,
                                      StreamConfig.PHYSICAL_NAME(), TEST_PHYSICAL_NAME2,
                                      StreamConfig.SYSTEM(), TEST_SYSTEM2);

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    env.streamFromConfig(STREAM_ID, TEST_PHYSICAL_NAME, null);
  }

  // Special characters are NOT allowed for streamId, because it's used as an identifier in the config.
  @Test(expected = IllegalArgumentException.class)
  public void testStreamFromConfigStreamIdInvalid() {
    Config config = buildStreamConfig(STREAM_ID_INVALID,
        StreamConfig.SYSTEM(), TEST_SYSTEM);

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    env.streamFromConfig(STREAM_ID_INVALID);
  }

  // Empty strings are NOT allowed for streamId, because it's used as an identifier in the config.
  @Test(expected = IllegalArgumentException.class)
  public void testStreamFromConfigStreamIdEmpty() {
    Config config = buildStreamConfig("",
        StreamConfig.SYSTEM(), TEST_SYSTEM);

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    env.streamFromConfig("");
  }

  // Null is not allowed for streamId.
  @Test(expected = NullPointerException.class)
  public void testStreamFromConfigStreamIdNull() {
    Config config = buildStreamConfig(null,
        StreamConfig.SYSTEM(), TEST_SYSTEM);

    ExecutionEnvironment env = new TestAbstractExecutionEnvironmentImpl(config);
    env.streamFromConfig(null);
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

  private class TestAbstractExecutionEnvironmentImpl extends AbstractExecutionEnvironment {

    public TestAbstractExecutionEnvironmentImpl(Config config) {
      super(config);
    }

    @Override
    public void run(StreamGraphBuilder graphBuilder, Config config) {
      // do nothing
    }
  }
}
