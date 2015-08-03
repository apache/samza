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
import scala.collection.JavaConversions._

object MetricsConfig {
  // metrics config constants
  val METRICS_REPORTERS = "metrics.reporters"
  val METRICS_REPORTER_FACTORY = "metrics.reporter.%s.class"
  val METRICS_SNAPSHOT_REPORTER_STREAM = "metrics.reporter.%s.stream"
  val METRICS_SNAPSHOT_REPORTER_INTERVAL= "metrics.reporter.%s.interval"

  implicit def Config2Metrics(config: Config) = new MetricsConfig(config)
}

class MetricsConfig(config: Config) extends ScalaMapConfig(config) {
  def getMetricsReporters(): Option[String] = getOption(MetricsConfig.METRICS_REPORTERS)

  def getMetricsFactoryClass(name: String): Option[String] = getOption(MetricsConfig.METRICS_REPORTER_FACTORY format name)

  def getMetricsReporterStream(name: String): Option[String] = getOption(MetricsConfig.METRICS_SNAPSHOT_REPORTER_STREAM format name)

  def getMetricsReporterInterval(name: String): Option[String] = getOption(MetricsConfig.METRICS_SNAPSHOT_REPORTER_INTERVAL format name)

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
}
