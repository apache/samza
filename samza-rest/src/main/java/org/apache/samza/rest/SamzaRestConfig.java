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
package org.apache.samza.rest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;


/**
 * The set of configurations required by the core components of the {@link org.apache.samza.rest.SamzaRestService}.
 * Other configurations (e.g. from {@link org.apache.samza.config.JobConfig}) may also be used by some of the
 * implementation classes.
 */
public class SamzaRestConfig extends MapConfig {
  /**
   * Specifies a comma-delimited list of class names that implement ResourceFactory.
   * These factories will be used to create specific instances of resources, passing the server config.
   */
  public static final String CONFIG_REST_RESOURCE_FACTORIES = "rest.resource.factory.classes";

  /**
   * Specifies a comma-delimited list of class names of resources to register with the server.
   */
  public static final String CONFIG_REST_RESOURCE_CLASSES = "rest.resource.classes";

  /**
   * Specifies a comma-delimited list of class names corresponding to Monitor implementations.
   * These will be instantiated and scheduled to run periodically at runtime.
   * Note that you must include the ENTIRE package name (org.apache.samza...).
   */
  public static final String CONFIG_MONITOR_CLASSES = "monitor.classes";

  /**
   * Specifies the interval at which each registered Monitor's monitor method will be called.
   */
  public static final String CONFIG_MONITOR_INTERVAL_MS = "monitor.run.interval.ms";

  /**
   * Monitors run every 60s by default
   */
  private static final int DEFAULT_MONITOR_INTERVAL = 60000;

  /**
   * The port number to use for the HTTP server or 0 to dynamically choose a port.
   */
  public static final String CONFIG_SAMZA_REST_SERVICE_PORT = "services.rest.port";

  public SamzaRestConfig(Config config) {
    super(config);
  }

  /**
   * @see SamzaRestConfig#CONFIG_SAMZA_REST_SERVICE_PORT
   * @return  the port number to use for the HTTP server or 0 to dynamically choose a port.
   */
  public int getPort() {
    return getInt(CONFIG_SAMZA_REST_SERVICE_PORT);
  }

  /**
   * @see SamzaRestConfig#CONFIG_REST_RESOURCE_FACTORIES
   * @return a list of class names as Strings corresponding to factories
   *          that Samza REST should use to instantiate and register resources
   *          or an empty list if none were configured.
   */
  public List<String> getResourceFactoryClassNames() {
    return parseCommaDelimitedStrings(get(CONFIG_REST_RESOURCE_FACTORIES));
  }

  /**
   * @see SamzaRestConfig#CONFIG_REST_RESOURCE_CLASSES
   * @return a list of class names as Strings corresponding to resource classes
   *          that Samza REST should register or an empty list if none were configured.
   */
  public List<String> getResourceClassNames() {
    return parseCommaDelimitedStrings(get(CONFIG_REST_RESOURCE_CLASSES));
  }

  /**
   * @see SamzaRestConfig#CONFIG_MONITOR_CLASSES
   * @return a list of class names as Strings corresponding to Monitors that
   *          Samza REST should schedule or an empty list if none were configured.
   */
  public List<String> getConfigMonitorClassList() {
    return parseCommaDelimitedStrings(get(CONFIG_MONITOR_CLASSES));
  }

  /**
   * @see SamzaRestConfig#CONFIG_MONITOR_INTERVAL_MS
   * @return an integer number of milliseconds, the period at which to schedule monitor runs.
   */
  public int getConfigMonitorIntervalMs() {
    return getInt(CONFIG_MONITOR_INTERVAL_MS, DEFAULT_MONITOR_INTERVAL);
  }

  /**
   * Parses a string containing a set of comma-delimited strings. Whitespace is ignored.
   * If the input string is null or empty, an empty list is returned.
   *
   * @param commaDelimitedStrings the string to parse.
   * @return                      the list of strings parsed from the input or an empty list if none.
   */
  private static List<String> parseCommaDelimitedStrings(String commaDelimitedStrings) {
    if (commaDelimitedStrings == null || commaDelimitedStrings.trim().isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.asList(commaDelimitedStrings.split("\\s*,\\s*"));
  }
}
