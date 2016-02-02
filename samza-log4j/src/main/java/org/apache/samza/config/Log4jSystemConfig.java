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

/**
 * This class contains the methods for getting properties that are needed by the
 * StreamAppender.
 */
public class Log4jSystemConfig extends JavaSystemConfig {

  private static final String LOCATION_ENABLED = "task.log4j.location.info.enabled";
  private static final String TASK_LOG4J_SYSTEM = "task.log4j.system";

  public Log4jSystemConfig(Config config) {
    super(config);
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
    return "true".equals(get(Log4jSystemConfig.LOCATION_ENABLED, "false"));
  }

  /**
   * Get the log4j system name from the config.
   * If it's not defined, throw a ConfigException
   *
   * @return log4j system name
   */
  public String getSystemName() {
    String log4jSystem = get(TASK_LOG4J_SYSTEM, null);
    if (log4jSystem == null) {
      throw new ConfigException("Missing " + TASK_LOG4J_SYSTEM + " configuration. Can't figure out the system name to use.");
    }
    return log4jSystem;
  }

  public String getJobName() {
    return get(JobConfig.JOB_NAME(), null);
  }

  public String getJobId() {
    return get(JobConfig.JOB_ID(), null);
  }

  /**
   * Get the class name according to the serde name.
   * 
   * @param name serde name
   * @return serde factory name, or null if there is no factory defined for the
   *         supplied serde name.
   */
  public String getSerdeClass(String name) {
    return get(String.format(SerializerConfig.SERDE(), name), null);
  }

  public String getStreamSerdeName(String systemName, String streamName) {
    String streamSerdeNameConfig = String.format(StreamConfig.MSG_SERDE(), systemName, streamName);
    return get(streamSerdeNameConfig, null);
  }
}
