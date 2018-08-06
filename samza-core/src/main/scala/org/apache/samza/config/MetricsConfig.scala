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


import org.apache.samza.system.SystemStream


object MetricsConfig {
  // metrics config constants
  val METRICS_REPORTERS = "metrics.reporters"
  val METRICS_REPORTER_FACTORY = "metrics.reporter.%s.class"
  val METRICS_SNAPSHOT_REPORTER_STREAM = "metrics.reporter.%s.stream"
  val METRICS_SNAPSHOT_REPORTER_INTERVAL = "metrics.reporter.%s.interval"
  val METRICS_TIMER_ENABLED = "metrics.timer.enabled"
  val METRICS_SNAPSHOT_REPORTER_BLACKLIST = "metrics.reporter.%s.blacklist"
  val METRICS_SNAPSHOT_REPORTER_PARTITION_KEY = "metrics.reporter.%s.partitionkey"
  val METRICS_SNAPSHOT_REPORTER_DEFAULT_PARTITION_KEY = "hostname"
  
  implicit def Config2Metrics(config: Config) = new MetricsConfig(config)
}

class MetricsConfig(config: Config) extends ScalaMapConfig(config) {
  def getMetricsReporters(): Option[String] = getOption(MetricsConfig.METRICS_REPORTERS)

  def getMetricsFactoryClass(name: String): Option[String] = getOption(MetricsConfig.METRICS_REPORTER_FACTORY format name)

  def getMetricsReporterStream(name: String): Option[String] = getOption(MetricsConfig.METRICS_SNAPSHOT_REPORTER_STREAM format name)

  def getMetricsReporterInterval(name: String): Option[String] = getOption(MetricsConfig.METRICS_SNAPSHOT_REPORTER_INTERVAL format name)

  def getBlacklist(name: String): Option[String] = getOption(MetricsConfig.METRICS_SNAPSHOT_REPORTER_BLACKLIST format name)

  /**
    * Parses the metric key name specified in config.
    * Customers can specify
    * hostname (for partitioning by hostname) or jobname or jobid
    * Partitioning by hostname is the default partitioning scheme.
    *
    * @param name The name of the reporter
    * @return the metric key name
    */
  def getMetricsReporterStreamPartitionKeyName(name: String): String =
    getOption(MetricsConfig.METRICS_SNAPSHOT_REPORTER_PARTITION_KEY format name).getOrElse(MetricsConfig.METRICS_SNAPSHOT_REPORTER_DEFAULT_PARTITION_KEY)


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
    *
    * @return Boolean flag to enable the timer metrics
    */
  def getMetricsTimerEnabled: Boolean = getBoolean(MetricsConfig.METRICS_TIMER_ENABLED, true)
}