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
  val METRICS_SNAPSHOT_REPORTER_PARTITION_KEY = "metrics.reporter.%s.partitionkey"
  val METRICS_SNAPSHOT_REPORTER_PARTITION_KEY_JOBNAME = "jobname"


  implicit def Config2Metrics(config: Config) = new MetricsConfig(config)
}

class MetricsConfig(config: Config) extends ScalaMapConfig(config) {
  def getMetricsReporters(): Option[String] = getOption(MetricsConfig.METRICS_REPORTERS)

  def getMetricsFactoryClass(name: String): Option[String] = getOption(MetricsConfig.METRICS_REPORTER_FACTORY format name)

  def getMetricsReporterStream(name: String): Option[String] = getOption(MetricsConfig.METRICS_SNAPSHOT_REPORTER_STREAM format name)

  def getMetricsReporterInterval(name: String): Option[String] = getOption(MetricsConfig.METRICS_SNAPSHOT_REPORTER_INTERVAL format name)

  /**
    * Parses the metric key name specified in config.
    * Customers can specify
    * hostname (for partitioning by hostname -- default) or jobname or jobid
    *
    * @param name The name of the reporter
    * @return the metric key name (if specified), else None
    */
  def getMetricsReporterStreamPartitionKeyName(name: String): Option[String] = getOption(MetricsConfig.METRICS_SNAPSHOT_REPORTER_PARTITION_KEY format name)

  /**
    * Returns the actual value of the partition key, based on the keyName.
    * If specified as "jobname" returns the jobname.
    * If none is specified or is specified as "hostname", returns the hostname,
    * else returns the actual value specified.
    *
    * @param keyName  the desired key name (hostname, jobname, none, or another custom name)
    * @param jobName  the current job name
    * @param hostname the current host name
    */
  def getMetricsReporterStreamPartitionKeyValue(keyName: Option[String], jobName: String, hostname: String) = keyName match {
    case Some(MetricsConfig.METRICS_SNAPSHOT_REPORTER_PARTITION_KEY_JOBNAME) => jobName
    case None => hostname
    case _ => keyName.get
  }


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