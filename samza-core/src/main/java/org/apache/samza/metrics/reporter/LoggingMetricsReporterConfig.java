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

package org.apache.samza.metrics.reporter;

import java.util.Optional;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;


public class LoggingMetricsReporterConfig extends MapConfig {
  private static final String METRICS_TO_LOG_REGEX_CONFIG = "metrics.reporter.%s.log.regex";
  private static final String LOGGING_INTERVAL_SECONDS_CONFIG = "metrics.reporter.%s.logging.interval.seconds";
  private static final long LOGGING_INTERVAL_SECONDS_DEFAULT = 60;

  public LoggingMetricsReporterConfig(Config config) {
    super(config);
  }

  public String getMetricsToLogRegex(String reporterName) {
    String metricsToLogConfigKey = String.format(METRICS_TO_LOG_REGEX_CONFIG, reporterName);
    return Optional.ofNullable(get(metricsToLogConfigKey))
        .orElseThrow(() -> new ConfigException("Missing value for " + metricsToLogConfigKey));
  }

  public long getLoggingIntervalSeconds(String reporterName) {
    return getLong(String.format(LOGGING_INTERVAL_SECONDS_CONFIG, reporterName), LOGGING_INTERVAL_SECONDS_DEFAULT);
  }
}
