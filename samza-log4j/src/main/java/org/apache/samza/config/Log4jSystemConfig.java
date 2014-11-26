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
import java.util.Map;

/**
 * This class contains the methods for getting properties that are needed by the
 * StreamAppender.
 */
public class Log4jSystemConfig {

  private static final String TASK_LOG4J_SYSTEM = "task.log4j.system";
  private static final String SYSTEM_PREFIX = "systems.";
  private static final String SYSTEM_FACTORY_SUFFIX = ".samza.factory";
  private static final String EMPTY = "";
  private Config config = null;

  public Log4jSystemConfig(Config config) {
    this.config = config;
  }

  /**
   * Get the log4j system name from the config. If it's not defined, try to
   * guess the system name if there is only one system is defined.
   *
   * @return log4j system name
   */
  public String getSystemName() {
    String log4jSystem = getValue(TASK_LOG4J_SYSTEM);
    if (log4jSystem == null) {
      ArrayList<String> systemNames = getSystemNames();
      if (systemNames.size() == 1) {
        log4jSystem = systemNames.get(0);
      } else {
        throw new ConfigException("Missing task.log4j.system configuration, and more than 1 systems were found.");
      }
    }
    return log4jSystem;
  }

  public String getJobName() {
    return getValue(JobConfig.JOB_NAME());
  }

  public String getJobId() {
    return getValue(JobConfig.JOB_ID());
  }

  public String getSystemFactory(String name) {
    if (name == null) {
      return null;
    }
    String systemFactory = String.format(SystemConfig.SYSTEM_FACTORY(), name);
    return getValue(systemFactory);
  }

  /**
   * a helper method to get the value from the config. If the config does not
   * contain the key, return null.
   *
   * @param key
   * @return value of the key in the config
   */
  protected String getValue(String key) {
    if (config.containsKey(key)) {
      return config.get(key);
    } else {
      return null;
    }
  }

  /**
   * get a list of system names
   */
  protected ArrayList<String> getSystemNames() {
    Config subConf = config.subset(SYSTEM_PREFIX, true);
    ArrayList<String> systemNames = new ArrayList<String>();
    for (Map.Entry<String, String> entry : subConf.entrySet()) {
      String key = entry.getKey();
      if (key.endsWith(SYSTEM_FACTORY_SUFFIX)) {
        systemNames.add(key.replace(SYSTEM_FACTORY_SUFFIX, EMPTY));
      }
    }
    return systemNames;
  }
}