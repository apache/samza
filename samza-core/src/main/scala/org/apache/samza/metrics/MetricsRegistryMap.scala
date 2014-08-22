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

import org.apache.samza.util.Logging
import java.util.concurrent.ConcurrentHashMap

/**
 * A class that holds all metrics registered with it. It can be registered
 * with one or more MetricReporters to flush metrics.
 */
class MetricsRegistryMap(val name: String) extends ReadableMetricsRegistry with Logging {
  var listeners = Set[ReadableMetricsRegistryListener]()

  /*
   * groupName -> metricName -> metric
   */
  val metrics = new ConcurrentHashMap[String, ConcurrentHashMap[String, Metric]]

  def this() = this("unknown")

  def newCounter(group: String, counter: Counter) = {
    debug("Add new counter %s %s %s." format (group, counter.getName, counter))
    putAndGetGroup(group).putIfAbsent(counter.getName, counter)
    val realCounter = metrics.get(group).get(counter.getName).asInstanceOf[Counter]
    listeners.foreach(_.onCounter(group, realCounter))
    realCounter
  }

  def newCounter(group: String, name: String) = {
    debug("Creating new counter %s %s." format (group, name))
    newCounter(group, new Counter(name))
  }

  def newGauge[T](group: String, gauge: Gauge[T]) = {
    debug("Adding new gauge %s %s %s." format (group, gauge.getName, gauge))
    putAndGetGroup(group).putIfAbsent(gauge.getName, gauge)
    val realGauge = metrics.get(group).get(gauge.getName).asInstanceOf[Gauge[T]]
    listeners.foreach(_.onGauge(group, realGauge))
    realGauge
  }

  def newGauge[T](group: String, name: String, value: T) = {
    debug("Creating new gauge %s %s %s." format (group, name, value))
    newGauge(group, new Gauge[T](name, value))
  }

  def newTimer(group: String, timer: Timer) = {
    debug("Add new timer %s %s %s." format (group, timer.getName, timer))
    putAndGetGroup(group).putIfAbsent(timer.getName, timer)
    val realTimer = metrics.get(group).get(timer.getName).asInstanceOf[Timer]
    listeners.foreach(_.onTimer(group, realTimer))
    realTimer
  }

  def newTimer(group: String, name: String) = {
    debug("Creating new timer %s %s." format (group, name))
    newTimer(group, new Timer(name))
  }

  private def putAndGetGroup(group: String) = {
    metrics.putIfAbsent(group, new ConcurrentHashMap[String, Metric])
    metrics.get(group)
  }

  def getName = name

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
