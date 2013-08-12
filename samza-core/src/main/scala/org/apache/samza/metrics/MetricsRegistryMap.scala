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

package org.apache.samza.metrics
import grizzled.slf4j.Logging
import java.util.concurrent.ConcurrentHashMap

/**
 * A class that holds all metrics registered with it. It can be registered
 * with one or more MetricReporters to flush metrics.
 */
class MetricsRegistryMap extends ReadableMetricsRegistry with Logging {
  var listeners = Set[ReadableMetricsRegistryListener]()

  /*
   * groupName -> metricName -> metric
   */
  val metrics = new ConcurrentHashMap[String, ConcurrentHashMap[String, Metric]]

  def newCounter(group: String, name: String) = {
    debug("Creating new counter %s %s." format (group, name))
    putAndGetGroup(group).putIfAbsent(name, new Counter(name))
    val counter = metrics.get(group).get(name).asInstanceOf[Counter]
    listeners.foreach(_.onCounter(group, counter))
    counter
  }

  def newGauge[T](group: String, name: String, value: T) = {
    debug("Creating new gauge %s %s %s." format (group, name, value))
    putAndGetGroup(group).putIfAbsent(name, new Gauge[T](name, value))
    val gauge = metrics.get(group).get(name).asInstanceOf[Gauge[T]]
    listeners.foreach(_.onGauge(group, gauge))
    gauge
  }

  private def putAndGetGroup(group: String) = {
    metrics.putIfAbsent(group, new ConcurrentHashMap[String, Metric])
    metrics.get(group)
  }

  def getGroups = metrics.keySet()

  def getGroup(group: String) = metrics.get(group)

  override def toString() = metrics.toString

  def listen(listener: ReadableMetricsRegistryListener) {
    listeners += listener
  }

  def unlisten(listener: ReadableMetricsRegistryListener) {
    listeners -= listener
  }
}
