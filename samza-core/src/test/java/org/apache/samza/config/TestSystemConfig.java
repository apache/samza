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

import com.google.common.collect.ImmutableMap;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestSystemConfig {
  private static final String MOCK_SYSTEM_NAME1 = "mocksystem1";
  private static final String MOCK_SYSTEM_NAME2 = "mocksystem2";
  private static final String MOCK_SYSTEM_FACTORY_NAME1 =
      String.format(SystemConfig.SYSTEM_FACTORY_FORMAT, MOCK_SYSTEM_NAME1);
  private static final String MOCK_SYSTEM_FACTORY_NAME2 =
      String.format(SystemConfig.SYSTEM_FACTORY_FORMAT, MOCK_SYSTEM_NAME2);
  private static final String MOCK_SYSTEM_FACTORY_CLASSNAME1 = "some.factory.Class1";
  private static final String MOCK_SYSTEM_FACTORY_CLASSNAME2 = "some.factory.Class2";

  private static final String SAMZA_OFFSET_DEFAULT = "samza.offset.default";

  /**
   * Placeholder to help make sure the correct {@link SystemAdmin} is returned by {@link MockSystemFactory}. Do not mock
   * any methods of this mock.
   */
  private static final SystemAdmin SYSTEM_ADMIN1 = mock(SystemAdmin.class);
  /**
   * Placeholder to help make sure the correct {@link SystemAdmin} is returned by {@link MockSystemFactory}. Do not mock
   * any methods of this mock.
   */
  private static final SystemAdmin SYSTEM_ADMIN2 = mock(SystemAdmin.class);

  @Test
  public void testGetSystemFactory() {
    Map<String, String> map = new HashMap<>();
    map.put(MOCK_SYSTEM_FACTORY_NAME1, MOCK_SYSTEM_FACTORY_CLASSNAME1);
    SystemConfig systemConfig = new SystemConfig(new MapConfig(map));

    assertEquals(MOCK_SYSTEM_FACTORY_CLASSNAME1, systemConfig.getSystemFactory(MOCK_SYSTEM_NAME1).get());
  }

  @Test
  public void testGetSystemFactoryEmptyClassName() {
    Map<String, String> map = new HashMap<>();
    map.put(MOCK_SYSTEM_FACTORY_NAME1, "");
    map.put(MOCK_SYSTEM_FACTORY_NAME2, " ");
    SystemConfig systemConfig = new SystemConfig(new MapConfig(map));

    assertFalse(systemConfig.getSystemFactory(MOCK_SYSTEM_NAME1).isPresent());
    assertFalse(systemConfig.getSystemFactory(MOCK_SYSTEM_NAME2).isPresent());
  }

  @Test
  public void testGetSystemNames() {
    Map<String, String> map = new HashMap<>();
    map.put(MOCK_SYSTEM_FACTORY_NAME1, MOCK_SYSTEM_FACTORY_CLASSNAME1);
    map.put(MOCK_SYSTEM_FACTORY_NAME2, MOCK_SYSTEM_FACTORY_CLASSNAME2);
    SystemConfig systemConfig = new SystemConfig(new MapConfig(map));

    assertEquals(2, systemConfig.getSystemNames().size());
    assertTrue(systemConfig.getSystemNames().contains(MOCK_SYSTEM_NAME1));
    assertTrue(systemConfig.getSystemNames().contains(MOCK_SYSTEM_NAME2));
  }

  @Test
  public void testGetSystemAdmins() {
    Map<String, String> map = ImmutableMap.of(MOCK_SYSTEM_FACTORY_NAME1, MockSystemFactory.class.getName());
    SystemConfig systemConfig = new SystemConfig(new MapConfig(map));
    Map<String, SystemAdmin> expected = ImmutableMap.of(MOCK_SYSTEM_NAME1, SYSTEM_ADMIN1);
    assertEquals(expected, systemConfig.getSystemAdmins());
  }

  @Test
  public void testGetSystemAdmin() {
    Map<String, String> map = ImmutableMap.of(MOCK_SYSTEM_FACTORY_NAME1, MockSystemFactory.class.getName());
    SystemConfig systemConfig = new SystemConfig(new MapConfig(map));
    assertEquals(SYSTEM_ADMIN1, systemConfig.getSystemAdmin(MOCK_SYSTEM_NAME1));
    assertNull(systemConfig.getSystemAdmin(MOCK_SYSTEM_NAME2));
  }

  @Test
  public void testGetSystemFactories() {
    Map<String, String> map = ImmutableMap.of(MOCK_SYSTEM_FACTORY_NAME1, MockSystemFactory.class.getName());
    SystemConfig systemConfig = new SystemConfig(new MapConfig(map));
    Map<String, SystemFactory> actual = systemConfig.getSystemFactories();
    assertEquals(actual.size(), 1);
    assertTrue(actual.get(MOCK_SYSTEM_NAME1) instanceof MockSystemFactory);
  }

  @Test
  public void testGetDefaultStreamProperties() {
    String defaultStreamPrefix = buildDefaultStreamPropertiesPrefix(MOCK_SYSTEM_NAME1);
    String system1ConfigKey = "config1-key";
    String system1ConfigValue = "config1-value";
    String system2ConfigKey = "config2-key";
    String system2ConfigValue = "config2-value";

    Config config = new MapConfig(ImmutableMap.of(defaultStreamPrefix + system1ConfigKey, system1ConfigValue,
        defaultStreamPrefix + system2ConfigKey, system2ConfigValue));
    Config expected =
        new MapConfig(ImmutableMap.of(system1ConfigKey, system1ConfigValue, system2ConfigKey, system2ConfigValue));
    SystemConfig systemConfig = new SystemConfig(config);
    assertEquals(expected, systemConfig.getDefaultStreamProperties(MOCK_SYSTEM_NAME1));
    assertEquals(new MapConfig(), systemConfig.getDefaultStreamProperties(MOCK_SYSTEM_NAME2));
  }

  @Test
  public void testGetSystemOffsetDefault() {
    String system1OffsetDefault = "offset-system-1";
    String system2OffsetDefault = "offset-system-2";
    Config config = new MapConfig(ImmutableMap.of(
        // default.stream.samza.offset.default set
        String.format(SystemConfig.SYSTEM_DEFAULT_STREAMS_PREFIX_FORMAT, MOCK_SYSTEM_NAME1) + SAMZA_OFFSET_DEFAULT,
        system1OffsetDefault,
        // should not use this value since default.stream.samza.offset.default is set
        String.format(SystemConfig.SYSTEM_ID_PREFIX, MOCK_SYSTEM_NAME1) + SAMZA_OFFSET_DEFAULT, "wrong-value",
        // only samza.offset.default set
        String.format(SystemConfig.SYSTEM_ID_PREFIX, MOCK_SYSTEM_NAME2) + SAMZA_OFFSET_DEFAULT, system2OffsetDefault));
    SystemConfig systemConfig = new SystemConfig(config);
    assertEquals(system1OffsetDefault, systemConfig.getSystemOffsetDefault(MOCK_SYSTEM_NAME1));
    assertEquals(system2OffsetDefault, systemConfig.getSystemOffsetDefault(MOCK_SYSTEM_NAME2));
    assertEquals(SystemConfig.SAMZA_SYSTEM_OFFSET_UPCOMING, systemConfig.getSystemOffsetDefault("other-system"));
  }

  @Test
  public void testGetSystemKeySerde() {
    String system1KeySerde = "system1-key-serde";
    String defaultStreamPrefixSystem1 = buildDefaultStreamPropertiesPrefix(MOCK_SYSTEM_NAME1);

    // value specified explicitly
    Config config =
        new MapConfig(ImmutableMap.of(defaultStreamPrefixSystem1 + StreamConfig.KEY_SERDE(), system1KeySerde));
    SystemConfig systemConfig = new SystemConfig(config);
    assertEquals(system1KeySerde, systemConfig.getSystemKeySerde(MOCK_SYSTEM_NAME1).get());

    // default stream property is unspecified, try fall back config key
    config = new MapConfig(
        ImmutableMap.of(String.format(SystemConfig.SYSTEM_ID_PREFIX, MOCK_SYSTEM_NAME1) + StreamConfig.KEY_SERDE(),
            system1KeySerde));
    systemConfig = new SystemConfig(config);
    assertEquals(system1KeySerde, systemConfig.getSystemKeySerde(MOCK_SYSTEM_NAME1).get());

    // default stream property is empty string, try fall back config key
    config = new MapConfig(ImmutableMap.of(
        // default stream property is empty
        defaultStreamPrefixSystem1 + StreamConfig.KEY_SERDE(), "",
        // fall back entry
        String.format(SystemConfig.SYSTEM_ID_PREFIX, MOCK_SYSTEM_NAME1) + StreamConfig.KEY_SERDE(), system1KeySerde));
    systemConfig = new SystemConfig(config);
    assertEquals(system1KeySerde, systemConfig.getSystemKeySerde(MOCK_SYSTEM_NAME1).get());

    // default stream property is unspecified, fall back is also empty
    config = new MapConfig(
        ImmutableMap.of(String.format(SystemConfig.SYSTEM_ID_PREFIX, MOCK_SYSTEM_NAME1) + StreamConfig.KEY_SERDE(),
            ""));
    systemConfig = new SystemConfig(config);
    assertFalse(systemConfig.getSystemKeySerde(MOCK_SYSTEM_NAME1).isPresent());

    // default stream property is unspecified, fall back is also unspecified
    config = new MapConfig();
    systemConfig = new SystemConfig(config);
    assertFalse(systemConfig.getSystemKeySerde(MOCK_SYSTEM_NAME1).isPresent());
  }

  @Test
  public void testGetSystemMsgSerde() {
    String system1MsgSerde = "system1-msg-serde";
    String defaultStreamPrefixSystem1 = buildDefaultStreamPropertiesPrefix(MOCK_SYSTEM_NAME1);

    // value specified explicitly
    Config config =
        new MapConfig(ImmutableMap.of(defaultStreamPrefixSystem1 + StreamConfig.MSG_SERDE(), system1MsgSerde));
    SystemConfig systemConfig = new SystemConfig(config);
    assertEquals(system1MsgSerde, systemConfig.getSystemMsgSerde(MOCK_SYSTEM_NAME1).get());

    // default stream property is unspecified, try fall back config msg
    config = new MapConfig(
        ImmutableMap.of(String.format(SystemConfig.SYSTEM_ID_PREFIX, MOCK_SYSTEM_NAME1) + StreamConfig.MSG_SERDE(),
            system1MsgSerde));
    systemConfig = new SystemConfig(config);
    assertEquals(system1MsgSerde, systemConfig.getSystemMsgSerde(MOCK_SYSTEM_NAME1).get());

    // default stream property is empty string, try fall back config msg
    config = new MapConfig(ImmutableMap.of(
        // default stream property is empty
        defaultStreamPrefixSystem1 + StreamConfig.MSG_SERDE(), "",
        // fall back entry
        String.format(SystemConfig.SYSTEM_ID_PREFIX, MOCK_SYSTEM_NAME1) + StreamConfig.MSG_SERDE(), system1MsgSerde));
    systemConfig = new SystemConfig(config);
    assertEquals(system1MsgSerde, systemConfig.getSystemMsgSerde(MOCK_SYSTEM_NAME1).get());

    // default stream property is unspecified, fall back is also empty
    config = new MapConfig(
        ImmutableMap.of(String.format(SystemConfig.SYSTEM_ID_PREFIX, MOCK_SYSTEM_NAME1) + StreamConfig.MSG_SERDE(),
            ""));
    systemConfig = new SystemConfig(config);
    assertFalse(systemConfig.getSystemMsgSerde(MOCK_SYSTEM_NAME1).isPresent());

    // default stream property is unspecified, fall back is also unspecified
    config = new MapConfig();
    systemConfig = new SystemConfig(config);
    assertFalse(systemConfig.getSystemMsgSerde(MOCK_SYSTEM_NAME1).isPresent());
  }

  @Test
  public void testDeleteCommittedMessages() {
    Config config = new MapConfig(ImmutableMap.of(
        // value is "true"
        String.format(SystemConfig.DELETE_COMMITTED_MESSAGES, MOCK_SYSTEM_NAME1), "true",
        // value is explicitly "false"
        String.format(SystemConfig.DELETE_COMMITTED_MESSAGES, MOCK_SYSTEM_NAME2), "false"));
    SystemConfig systemConfig = new SystemConfig(config);
    assertTrue(systemConfig.deleteCommittedMessages(MOCK_SYSTEM_NAME1));
    assertFalse(systemConfig.deleteCommittedMessages(MOCK_SYSTEM_NAME2));
    assertFalse(systemConfig.deleteCommittedMessages("other-system")); // value is not specified
  }

  public static class MockSystemFactory implements SystemFactory {
    @Override
    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
      throw new UnsupportedOperationException("Unnecessary for test");
    }

    @Override
    public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
      throw new UnsupportedOperationException("Unnecessary for test");
    }

    @Override
    public SystemAdmin getAdmin(String systemName, Config config) {
      switch (systemName) {
        case MOCK_SYSTEM_NAME1:
          return SYSTEM_ADMIN1;
        case MOCK_SYSTEM_NAME2:
          return SYSTEM_ADMIN2;
        default:
          throw new UnsupportedOperationException("System name unsupported: " + systemName);
      }
    }
  }

  private static String buildDefaultStreamPropertiesPrefix(String systemName) {
    return String.format(SystemConfig.SYSTEM_DEFAULT_STREAMS_PREFIX_FORMAT, systemName);
  }
}
