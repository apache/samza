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
package org.apache.samza.monitor;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;

/**
 * Configurations for the {@link Monitor} implementations.
 */
public class MonitorConfig extends MapConfig {

  public static final String CONFIG_SCHEDULING_INTERVAL = "scheduling.interval.ms";

  public static final String CONFIG_MONITOR_FACTORY_CLASS = "factory.class";

  private static final int DEFAULT_SCHEDULING_INTERVAL_IN_MS = 60000;

  private static final String MONITOR_CONFIG_KEY_SEPARATOR = ".";

  private static final String MONITOR_PREFIX = String.format("monitor%s", MONITOR_CONFIG_KEY_SEPARATOR);

  public MonitorConfig(Config config) {
    super(config);
  }

  /**
   *
   * Groups configuration defined in the config object for each of the monitors into a MonitorConfig object
   * @param config contains the entire configuration defined for all the monitors.
   * @return a map of monitorName, {@link MonitorConfig}, where each MonitorConfig object
   * contains all the configuration defined for the monitor named monitorName.
   */
  public static Map<String, MonitorConfig> getMonitorConfigs(Config config) {
    Map<String, MonitorConfig> monitorConfigMap = new HashMap<>();
    Config monitorConfig = config.subset(MONITOR_PREFIX);
    for (String monitorName : getMonitorNames(monitorConfig)) {
      monitorConfigMap.put(monitorName,
                           new MonitorConfig(monitorConfig.subset(monitorName + MONITOR_CONFIG_KEY_SEPARATOR)));
    }
    return monitorConfigMap;
  }

  /**
   *
   * @param config contains all the configuration that are defined for the monitors.
   * @return a unique collection of monitor names for which configuration has been defined in the config object
   */
  private static Set<String> getMonitorNames(Config config) {
    Set<String> monitorNames = new HashSet<>();
    for (String configKey : config.keySet()) {
      String[] configKeyComponents = StringUtils.split(configKey, MONITOR_CONFIG_KEY_SEPARATOR);
      Preconditions.checkState(configKeyComponents.length != 0);
      String monitorName = configKeyComponents[0];
      monitorNames.add(monitorName);
    }
    return monitorNames;
  }

  /**
   *
   * @return the monitor config factory class name that will be used to create
   * the monitor instances.
   */
  public String getMonitorFactoryClass() {
    return get(CONFIG_MONITOR_FACTORY_CLASS);
  }

  /**
   *
   * @return the periodic scheduling interval defined in the Config. If the configuration
   * key is undefined, return the default value({@link MonitorConfig#DEFAULT_SCHEDULING_INTERVAL_IN_MS}).
   */
  public int getSchedulingIntervalInMs() {
    return getInt(CONFIG_SCHEDULING_INTERVAL, DEFAULT_SCHEDULING_INTERVAL_IN_MS);
  }
}
