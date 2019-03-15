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
import com.google.common.collect.ImmutableMap;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;


public class TestJavaSystemConfig {
  private static final String MOCK_SYSTEM_NAME1 = "mocksystem1";
  private static final String MOCK_SYSTEM_NAME2 = "mocksystem2";
  private static final String MOCK_SYSTEM_FACTORY_NAME1 =
      String.format(JavaSystemConfig.SYSTEM_FACTORY_FORMAT, MOCK_SYSTEM_NAME1);
  private static final String MOCK_SYSTEM_FACTORY_NAME2 =
      String.format(JavaSystemConfig.SYSTEM_FACTORY_FORMAT, MOCK_SYSTEM_NAME2);
  private static final String MOCK_SYSTEM_FACTORY_CLASSNAME1 = "some.factory.Class1";
  private static final String MOCK_SYSTEM_FACTORY_CLASSNAME2 = "some.factory.Class2";

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
    JavaSystemConfig systemConfig = new JavaSystemConfig(new MapConfig(map));

    assertEquals(MOCK_SYSTEM_FACTORY_CLASSNAME1, systemConfig.getSystemFactory(MOCK_SYSTEM_NAME1).get());
  }

  @Test
  public void testGetSystemFactoryEmptyClassName() {
    Map<String, String> map = new HashMap<>();
    map.put(MOCK_SYSTEM_FACTORY_NAME1, "");
    map.put(MOCK_SYSTEM_FACTORY_NAME2, " ");
    JavaSystemConfig systemConfig = new JavaSystemConfig(new MapConfig(map));

    assertFalse(systemConfig.getSystemFactory(MOCK_SYSTEM_NAME1).isPresent());
    assertFalse(systemConfig.getSystemFactory(MOCK_SYSTEM_NAME2).isPresent());
  }

  @Test
  public void testGetSystemNames() {
    Map<String, String> map = new HashMap<>();
    map.put(MOCK_SYSTEM_FACTORY_NAME1, MOCK_SYSTEM_FACTORY_CLASSNAME1);
    map.put(MOCK_SYSTEM_FACTORY_NAME2, MOCK_SYSTEM_FACTORY_CLASSNAME2);
    JavaSystemConfig systemConfig = new JavaSystemConfig(new MapConfig(map));

    assertEquals(2, systemConfig.getSystemNames().size());
    assertTrue(systemConfig.getSystemNames().contains(MOCK_SYSTEM_NAME1));
    assertTrue(systemConfig.getSystemNames().contains(MOCK_SYSTEM_NAME2));
  }

  @Test
  public void testGetSystemAdmins() {
    Map<String, String> map = ImmutableMap.of(MOCK_SYSTEM_FACTORY_NAME1, MockSystemFactory.class.getName());
    JavaSystemConfig systemConfig = new JavaSystemConfig(new MapConfig(map));
    Map<String, SystemAdmin> expected = ImmutableMap.of(MOCK_SYSTEM_NAME1, SYSTEM_ADMIN1);
    assertEquals(expected, systemConfig.getSystemAdmins());
  }

  @Test
  public void testGetSystemAdmin() {
    Map<String, String> map = ImmutableMap.of(MOCK_SYSTEM_FACTORY_NAME1, MockSystemFactory.class.getName());
    JavaSystemConfig systemConfig = new JavaSystemConfig(new MapConfig(map));
    assertEquals(SYSTEM_ADMIN1, systemConfig.getSystemAdmin(MOCK_SYSTEM_NAME1));
    assertNull(systemConfig.getSystemAdmin(MOCK_SYSTEM_NAME2));
  }

  @Test
  public void testGetSystemFactories() {
    Map<String, String> map = ImmutableMap.of(MOCK_SYSTEM_FACTORY_NAME1, MockSystemFactory.class.getName());
    JavaSystemConfig systemConfig = new JavaSystemConfig(new MapConfig(map));
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
    JavaSystemConfig systemConfig = new JavaSystemConfig(config);
    assertEquals(expected, systemConfig.getDefaultStreamProperties(MOCK_SYSTEM_NAME1));
    assertEquals(new MapConfig(), systemConfig.getDefaultStreamProperties(MOCK_SYSTEM_NAME2));
  }

  @Test
  public void testGetSystemOffsetDefault() {
    String system1OffsetDefault = "offset-system-1";
    String system2OffsetDefault = "offset-system-2";
    Config config = new MapConfig(ImmutableMap.of(
        // default.stream.samza.offset.default set
        String.format("systems.%s.default.stream.samza.offset.default", MOCK_SYSTEM_NAME1), system1OffsetDefault,
        // should not use this value since default.stream.samza.offset.default is set
        String.format("systems.%s.samza.offset.default", MOCK_SYSTEM_NAME1), "wrong-value",
        // only samza.offset.default set
        String.format("systems.%s.samza.offset.default", MOCK_SYSTEM_NAME2), system2OffsetDefault));
    JavaSystemConfig systemConfig = new JavaSystemConfig(config);
    assertEquals(system1OffsetDefault, systemConfig.getSystemOffsetDefault(MOCK_SYSTEM_NAME1));
    assertEquals(system2OffsetDefault, systemConfig.getSystemOffsetDefault(MOCK_SYSTEM_NAME2));
    assertEquals(JavaSystemConfig.SAMZA_SYSTEM_OFFSET_UPCOMING, systemConfig.getSystemOffsetDefault("other-system"));
  }

  @Test
  public void testGetSystemKeySerde() {
    String system1KeySerde = "system1-key-serde";
    String defaultStreamPrefixSystem1 = buildDefaultStreamPropertiesPrefix(MOCK_SYSTEM_NAME1);
    String samzaKeySerdeSuffix = "samza.key.serde";

    // value specified explicitly
    Config config = new MapConfig(ImmutableMap.of(defaultStreamPrefixSystem1 + samzaKeySerdeSuffix, system1KeySerde));
    JavaSystemConfig systemConfig = new JavaSystemConfig(config);
    assertEquals(system1KeySerde, systemConfig.getSystemKeySerde(MOCK_SYSTEM_NAME1).get());

    // default stream property is unspecified, try fall back config key
    config = new MapConfig(
        ImmutableMap.of(String.format("systems.%s.%s", MOCK_SYSTEM_NAME1, samzaKeySerdeSuffix), system1KeySerde));
    systemConfig = new JavaSystemConfig(config);
    assertEquals(system1KeySerde, systemConfig.getSystemKeySerde(MOCK_SYSTEM_NAME1).get());

    // default stream property is empty string, try fall back config key
    config = new MapConfig(ImmutableMap.of(
        // default stream property is empty
        defaultStreamPrefixSystem1 + samzaKeySerdeSuffix, "",
        // fall back entry
        String.format("systems.%s.%s", MOCK_SYSTEM_NAME1, samzaKeySerdeSuffix), system1KeySerde));
    systemConfig = new JavaSystemConfig(config);
    assertEquals(system1KeySerde, systemConfig.getSystemKeySerde(MOCK_SYSTEM_NAME1).get());

    // default stream property is unspecified, fall back is also empty
    config = new MapConfig(ImmutableMap.of(String.format("systems.%s.%s", MOCK_SYSTEM_NAME1, samzaKeySerdeSuffix), ""));
    systemConfig = new JavaSystemConfig(config);
    assertFalse(systemConfig.getSystemKeySerde(MOCK_SYSTEM_NAME1).isPresent());

    // default stream property is unspecified, fall back is also unspecified
    config = new MapConfig();
    systemConfig = new JavaSystemConfig(config);
    assertFalse(systemConfig.getSystemKeySerde(MOCK_SYSTEM_NAME1).isPresent());
  }

  @Test
  public void testGetSystemMsgSerde() {
    String system1MsgSerde = "system1-msg-serde";
    String defaultStreamPrefixSystem1 = buildDefaultStreamPropertiesPrefix(MOCK_SYSTEM_NAME1);
    String samzaMsgSerdeSuffix = "samza.msg.serde";

    // value specified explicitly
    Config config = new MapConfig(ImmutableMap.of(defaultStreamPrefixSystem1 + samzaMsgSerdeSuffix, system1MsgSerde));
    JavaSystemConfig systemConfig = new JavaSystemConfig(config);
    assertEquals(system1MsgSerde, systemConfig.getSystemMsgSerde(MOCK_SYSTEM_NAME1).get());

    // default stream property is unspecified, try fall back config msg
    config = new MapConfig(
        ImmutableMap.of(String.format("systems.%s.%s", MOCK_SYSTEM_NAME1, samzaMsgSerdeSuffix), system1MsgSerde));
    systemConfig = new JavaSystemConfig(config);
    assertEquals(system1MsgSerde, systemConfig.getSystemMsgSerde(MOCK_SYSTEM_NAME1).get());

    // default stream property is empty string, try fall back config msg
    config = new MapConfig(ImmutableMap.of(
        // default stream property is empty
        defaultStreamPrefixSystem1 + samzaMsgSerdeSuffix, "",
        // fall back entry
        String.format("systems.%s.%s", MOCK_SYSTEM_NAME1, samzaMsgSerdeSuffix), system1MsgSerde));
    systemConfig = new JavaSystemConfig(config);
    assertEquals(system1MsgSerde, systemConfig.getSystemMsgSerde(MOCK_SYSTEM_NAME1).get());

    // default stream property is unspecified, fall back is also empty
    config = new MapConfig(ImmutableMap.of(String.format("systems.%s.%s", MOCK_SYSTEM_NAME1, samzaMsgSerdeSuffix), ""));
    systemConfig = new JavaSystemConfig(config);
    assertFalse(systemConfig.getSystemMsgSerde(MOCK_SYSTEM_NAME1).isPresent());

    // default stream property is unspecified, fall back is also unspecified
    config = new MapConfig();
    systemConfig = new JavaSystemConfig(config);
    assertFalse(systemConfig.getSystemMsgSerde(MOCK_SYSTEM_NAME1).isPresent());
  }

  @Test
  public void testDeleteCommittedMessages() {
    String deleteCommittedMessagesFormat = "systems.%s.samza.delete.committed.messages";
    Config config = new MapConfig(ImmutableMap.of(
        // value is "true"
        String.format(deleteCommittedMessagesFormat, MOCK_SYSTEM_NAME1), "true",
        // value is explicitly "false"
        String.format(deleteCommittedMessagesFormat, MOCK_SYSTEM_NAME2), "false"));
    JavaSystemConfig systemConfig = new JavaSystemConfig(config);
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
    return String.format("systems.%s.default.stream.", systemName);
  }
}
