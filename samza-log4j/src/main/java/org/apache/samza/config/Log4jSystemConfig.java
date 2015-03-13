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

/**
 * This class contains the methods for getting properties that are needed by the
 * StreamAppender.
 */
public class Log4jSystemConfig {

  private static final String LOCATION_ENABLED = "task.log4j.location.info.enabled";
  private static final String TASK_LOG4J_SYSTEM = "task.log4j.system";
  private static final String SYSTEM_PREFIX = "systems.";
  private static final String SYSTEM_FACTORY_SUFFIX = ".samza.factory";
  private static final String EMPTY = "";
  private Config config = null;

  public Log4jSystemConfig(Config config) {
    this.config = config;
  }

  /**
   * Defines whether or not to include file location information for Log4J
   * appender messages. File location information includes the method, line
   * number, class, etc.
   * 
   * @return If true, will include file location (method, line number, etc)
   *         information in Log4J appender messages.
   */
  public boolean getLocationEnabled() {
    return "true".equals(config.get(Log4jSystemConfig.LOCATION_ENABLED, "false"));
  }

  /**
   * Get the log4j system name from the config. If it's not defined, try to
   * guess the system name if there is only one system is defined.
   *
   * @return log4j system name
   */
  public String getSystemName() {
    String log4jSystem = config.get(TASK_LOG4J_SYSTEM, null);
    if (log4jSystem == null) {
      List<String> systemNames = getSystemNames();
      if (systemNames.size() == 1) {
        log4jSystem = systemNames.get(0);
      } else {
        throw new ConfigException("Missing " + TASK_LOG4J_SYSTEM + " configuration, and more than 1 systems were found.");
      }
    }
    return log4jSystem;
  }

  public String getJobName() {
    return config.get(JobConfig.JOB_NAME(), null);
  }

  public String getJobId() {
    return config.get(JobConfig.JOB_ID(), null);
  }

  public String getSystemFactory(String name) {
    if (name == null) {
      return null;
    }
    String systemFactory = String.format(SystemConfig.SYSTEM_FACTORY(), name);
    return config.get(systemFactory, null);
  }

  /**
   * Get the class name according to the serde name.
   * 
   * @param name
   *          serde name
   * @return serde factory name, or null if there is no factory defined for the
   *         supplied serde name.
   */
  public String getSerdeClass(String name) {
    return config.get(String.format(SerializerConfig.SERDE(), name), null);
  }

  public String getStreamSerdeName(String systemName, String streamName) {
    String streamSerdeNameConfig = String.format(StreamConfig.MSG_SERDE(), systemName, streamName);
    return config.get(streamSerdeNameConfig, null);
  }

  /**
   * Get a list of system names.
   * 
   * @return A list system names
   */
  protected List<String> getSystemNames() {
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
