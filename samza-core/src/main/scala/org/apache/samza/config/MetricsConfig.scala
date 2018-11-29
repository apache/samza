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

package org.apache.samza.config


import org.apache.samza.util.HighResolutionClock


object MetricsConfig {
  // metrics config constants
  val METRICS_REPORTERS = "metrics.reporters"
  val METRICS_REPORTER_FACTORY = "metrics.reporter.%s.class"
  // This flag enables the common timer metrics, e.g. process_ns
  val METRICS_TIMER_ENABLED= "metrics.timer.enabled"
  // This flag enables more timer metrics, e.g. handle-message-ns in an operator, for debugging purpose
  val METRICS_TIMER_DEBUG_ENABLED= "metrics.timer.debug.enabled"

  // The following configs are applicable only to {@link MetricsSnapshotReporter}
  // added here only to maintain backwards compatibility of config
  val METRICS_SNAPSHOT_REPORTER_STREAM = "metrics.reporter.%s.stream"
  val METRICS_SNAPSHOT_REPORTER_INTERVAL= "metrics.reporter.%s.interval"
  val METRICS_SNAPSHOT_REPORTER_BLACKLIST = "metrics.reporter.%s.blacklist"
  val METRICS_SNAPSHOT_REPORTER_NAME_FOR_DIAGNOSTICS = "diagnosticsreporter"

  implicit def Config2Metrics(config: Config) = new MetricsConfig(config)
}

class MetricsConfig(config: Config) extends ScalaMapConfig(config) {
  def getMetricsReporters(): Option[String] = getOption(MetricsConfig.METRICS_REPORTERS)

  def getMetricsFactoryClass(name: String): Option[String] = getOption(MetricsConfig.METRICS_REPORTER_FACTORY format name)

  def getMetricsSnapshotReporterStream(name: String): Option[String] = getOption(MetricsConfig.METRICS_SNAPSHOT_REPORTER_STREAM format name)

  def getMetricsSnapshotReporterInterval(name: String): Option[String] = getOption(MetricsConfig.METRICS_SNAPSHOT_REPORTER_INTERVAL format name)

  def getMetricsSnapshotReporterBlacklist(name: String): Option[String] = getOption(MetricsConfig.METRICS_SNAPSHOT_REPORTER_BLACKLIST format name)

  /**
   * Returns a list of all metrics names from the config file. Useful for
   * getting individual metrics.
   */
  def getMetricReporterNames() = {
    getMetricsReporters match {
      case Some(mr) => if (!"".equals(mr)) {
        mr.split(",").map(name => name.trim).toList
      } else {
        List[String]()
      }
      case _ => List[String]()
    }
  }

  /**
   * Returns the flag to turn on/off the timer metrics.
   * @return Boolean flag to enable the timer metrics
   */
  def getMetricsTimerEnabled: Boolean = getBoolean(MetricsConfig.METRICS_TIMER_ENABLED, true)

  /**
    * Returns the flag to enable the debug timer metrics. These metrics
    * are turned off by default for better performance.
    * @return Boolean
    */
  def getMetricsTimerDebugEnabled: Boolean = getBoolean(MetricsConfig.METRICS_TIMER_DEBUG_ENABLED, false)

}