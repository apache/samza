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
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.util.Util;


/**
 * a java version of the system config
 */
public class JavaSystemConfig extends MapConfig {
  public static final String SYSTEM_PREFIX = "systems.";
  public static final String SYSTEM_FACTORY_SUFFIX = ".samza.factory";
  public static final String SYSTEM_FACTORY_FORMAT = SYSTEM_PREFIX + "%s" + SYSTEM_FACTORY_SUFFIX;
  private static final String SYSTEM_DEFAULT_STREAMS_PREFIX_FORMAT = SYSTEM_PREFIX + "%s" + ".default.stream.";
  private static final String EMPTY = "";

  public JavaSystemConfig(Config config) {
    super(config);
  }

  public String getSystemFactory(String name) {
    if (name == null) {
      return null;
    }
    String systemFactory = String.format(SYSTEM_FACTORY_FORMAT, name);
    String value = get(systemFactory, null);
    return (StringUtils.isBlank(value)) ? null : value;
  }

  /**
   * Get a list of system names.
   *
   * @return A list system names
   */
  public List<String> getSystemNames() {
    Config subConf = subset(SYSTEM_PREFIX, true);
    ArrayList<String> systemNames = new ArrayList<String>();
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
        .collect(Collectors.toMap(systemNameToFactoryEntry -> systemNameToFactoryEntry.getKey(),
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
        String systemFactoryClassName = getSystemFactory(systemName);
        if (systemFactoryClassName == null) {
          throw new SamzaException(
              String.format("A stream uses system %s, which is missing from the configuration.", systemName));
        }
        return Util.getObj(systemFactoryClassName);
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
}
