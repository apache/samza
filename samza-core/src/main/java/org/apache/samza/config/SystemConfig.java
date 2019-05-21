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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.util.Util;

/**
 * Config helper methods related to systems.
 */
public class SystemConfig extends MapConfig {
  private static final String SYSTEMS_PREFIX = "systems.";
  public static final String SYSTEM_ID_PREFIX = SYSTEMS_PREFIX + "%s.";

  private static final String SYSTEM_FACTORY_SUFFIX = ".samza.factory";
  public static final String SYSTEM_FACTORY_FORMAT = SYSTEMS_PREFIX + "%s" + SYSTEM_FACTORY_SUFFIX;
  @VisibleForTesting
  static final String SYSTEM_DEFAULT_STREAMS_PREFIX_FORMAT = SYSTEM_ID_PREFIX + "default.stream.";

  // If true, automatically delete committed messages from streams whose committed messages can be deleted.
  // A stream's committed messages can be deleted if it is a intermediate stream, or if user has manually
  // set streams.{streamId}.samza.delete.committed.messages to true in the configuration.
  @VisibleForTesting
  static final String DELETE_COMMITTED_MESSAGES = SYSTEM_ID_PREFIX + "samza.delete.committed.messages";

  private static final String EMPTY = "";

  static final String SAMZA_SYSTEM_OFFSET_UPCOMING = "upcoming";
  static final String SAMZA_SYSTEM_OFFSET_OLDEST = "oldest";

  public SystemConfig(Config config) {
    super(config);
  }

  public Optional<String> getSystemFactory(String systemName) {
    if (systemName == null) {
      return Optional.empty();
    }
    String systemFactory = String.format(SYSTEM_FACTORY_FORMAT, systemName);
    String value = get(systemFactory, null);
    return (StringUtils.isBlank(value)) ? Optional.empty() : Optional.of(value);
  }

  /**
   * Get a list of system names.
   *
   * @return A list system names
   */
  public List<String> getSystemNames() {
    Config subConf = subset(SYSTEMS_PREFIX, true);
    ArrayList<String> systemNames = new ArrayList<>();
    for (Map.Entry<String, String> entry : subConf.entrySet()) {
      String key = entry.getKey();
      if (key.endsWith(SYSTEM_FACTORY_SUFFIX)) {
        systemNames.add(key.replace(SYSTEM_FACTORY_SUFFIX, EMPTY));
      }
    }
    return systemNames;
  }

  /**
   * Get {@link SystemAdmin} instances for all the systems defined in this config.
   *
   * @return map of system name to {@link SystemAdmin}
   */
  public Map<String, SystemAdmin> getSystemAdmins() {
    return getSystemFactories().entrySet()
        .stream()
        .collect(Collectors.toMap(Entry::getKey,
            systemNameToFactoryEntry -> systemNameToFactoryEntry.getValue()
                .getAdmin(systemNameToFactoryEntry.getKey(), this)));
  }

  /**
   * Get {@link SystemAdmin} instance for given system name.
   *
   * @param systemName System name
   * @return SystemAdmin of the system if it exists, otherwise null.
   */
  public SystemAdmin getSystemAdmin(String systemName) {
    return getSystemAdmins().get(systemName);
  }

  /**
   * Get {@link SystemFactory} instances for all the systems defined in this config.
   *
   * @return a map from system name to {@link SystemFactory}
   */
  public Map<String, SystemFactory> getSystemFactories() {
    Map<String, SystemFactory> systemFactories = getSystemNames().stream().collect(Collectors.toMap(
      systemName -> systemName,
      systemName -> {
        String systemFactoryClassName = getSystemFactory(systemName).orElseThrow(() -> new SamzaException(
            String.format("A stream uses system %s, which is missing from the configuration.", systemName)));
        return Util.getObj(systemFactoryClassName, SystemFactory.class);
      }));

    return systemFactories;
  }

  /**
   * Gets the system-wide defaults for streams.
   *
   * @param systemName the name of the system for which the defaults will be returned.
   * @return a subset of the config with the system prefix removed.
   */
  public Config getDefaultStreamProperties(String systemName) {
    return subset(String.format(SYSTEM_DEFAULT_STREAMS_PREFIX_FORMAT, systemName), true);
  }

  /**
   * Get system offset default value.
   * systems.'system'.default.stream.samza.offset.default is the config.
   * systems.'system'.samza.offset.default is the deprecated setting, but needs to be checked for backward compatibility.
   * @param systemName get config value for this system.
   * @return value of system reset or default ("upcoming") if none set.
   */
  public String getSystemOffsetDefault(String systemName) {
    // first check stream system default
    String systemOffsetDefault = get(String.format("systems.%s.default.stream.samza.offset.default", systemName));

    // if not set, check the deprecated setting
    if (StringUtils.isBlank(systemOffsetDefault)) {
      systemOffsetDefault = get(String.format("systems.%s.samza.offset.default", systemName));
      if (StringUtils.isBlank(systemOffsetDefault)) {
        return SAMZA_SYSTEM_OFFSET_UPCOMING;
      }
    }

    return systemOffsetDefault;
  }

  /**
   * @param systemName name of the system
   * @return the key serde for the {@code systemName}, or empty if it was not found
   */
  public Optional<String> getSystemKeySerde(String systemName) {
    return getSystemDefaultStreamProperty(systemName, StreamConfig.KEY_SERDE());
  }

  /**
   * @param systemName name of the system
   * @return the message serde for the {@code systemName}, or empty if it was not found
   */
  public Optional<String> getSystemMsgSerde(String systemName) {
    return getSystemDefaultStreamProperty(systemName, StreamConfig.MSG_SERDE());
  }

  /**
   * @param systemName name of the system
   * @return if messages committed to this system should automatically be deleted
   */
  public boolean deleteCommittedMessages(String systemName) {
    return getBoolean(String.format(DELETE_COMMITTED_MESSAGES, systemName), false);
  }

  /**
   * Gets the system-wide default for the {@code propertyName} for the {@code systemName}.
   * This will check in a couple of different config locations for the value.
   */
  private Optional<String> getSystemDefaultStreamProperty(String systemName, String propertyName) {
    Map<String, String> defaultStreamProperties = getDefaultStreamProperties(systemName);
    String defaultStreamProperty = defaultStreamProperties.get(propertyName);
    if (StringUtils.isNotEmpty(defaultStreamProperty)) {
      return Optional.of(defaultStreamProperty);
    } else {
      String fallbackStreamProperty = get(String.format(SYSTEM_ID_PREFIX, systemName) + propertyName);
      if (StringUtils.isNotEmpty(fallbackStreamProperty)) {
        return Optional.of(fallbackStreamProperty);
      } else {
        return Optional.empty();
      }
    }
  }
}
