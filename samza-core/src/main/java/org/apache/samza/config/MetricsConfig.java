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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Helper class for accessing configs related to metrics.
 */
public class MetricsConfig extends MapConfig {
  // metrics config constants
  public static final String METRICS_REPORTERS = "metrics.reporters";
  public static final String METRICS_REPORTER_FACTORY = "metrics.reporter.%s.class";
  // This flag enables the common timer metrics, e.g. process_ns
  public static final String METRICS_TIMER_ENABLED = "metrics.timer.enabled";
  // This flag enables more timer metrics, e.g. handle-message-ns in an operator, for debugging purpose
  public static final String METRICS_TIMER_DEBUG_ENABLED = "metrics.timer.debug.enabled";

  // The following configs are applicable only to {@link MetricsSnapshotReporter}
  // added here only to maintain backwards compatibility of config
  public static final String METRICS_SNAPSHOT_REPORTER_STREAM = "metrics.reporter.%s.stream";
  // unit for this config is seconds
  public static final String METRICS_SNAPSHOT_REPORTER_INTERVAL = "metrics.reporter.%s.interval";
  static final int DEFAULT_METRICS_SNAPSHOT_REPORTER_INTERVAL = 60;
  public static final String METRICS_SNAPSHOT_REPORTER_BLACKLIST = "metrics.reporter.%s.blacklist";
  public static final String METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS = "diagnosticsreporter";

  public MetricsConfig(Config config) {
    super(config);
  }

  public Optional<String> getMetricsFactoryClass(String name) {
    return Optional.ofNullable(get(String.format(METRICS_REPORTER_FACTORY, name)));
  }

  public Optional<String> getMetricsSnapshotReporterStream(String name) {
    return Optional.ofNullable(get(String.format(METRICS_SNAPSHOT_REPORTER_STREAM, name)));
  }

  public int getMetricsSnapshotReporterInterval(String name) {
    return getInt(String.format(METRICS_SNAPSHOT_REPORTER_INTERVAL, name), DEFAULT_METRICS_SNAPSHOT_REPORTER_INTERVAL);
  }

  public Optional<String> getMetricsSnapshotReporterBlacklist(String name) {
    return Optional.ofNullable(get(String.format(METRICS_SNAPSHOT_REPORTER_BLACKLIST, name)));
  }

  public List<String> getMetricReporterNames() {
    Optional<String> metricReporterNamesValue = Optional.ofNullable(get(METRICS_REPORTERS));
    if (!metricReporterNamesValue.isPresent() || metricReporterNamesValue.get().isEmpty()) {
      return Collections.emptyList();
    } else {
      return Stream.of(metricReporterNamesValue.get().split(",")).map(String::trim).collect(Collectors.toList());
    }
  }

  public boolean getMetricsTimerEnabled() {
    return getBoolean(METRICS_TIMER_ENABLED, true);
  }

  public boolean getMetricsTimerDebugEnabled() {
    return getBoolean(METRICS_TIMER_DEBUG_ENABLED, false);
  }
}
