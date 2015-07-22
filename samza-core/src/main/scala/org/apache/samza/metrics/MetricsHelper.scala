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

import org.apache.samza.metrics.MetricGroup.ValueFunction


/**
 * MetricsHelper is a little helper class to make it easy to register and
 * manage counters, gauges and timers.
 *
 * The name of the class that extends this trait will be used as the
 * metric group name
 */
trait MetricsHelper {
  val group = this.getClass.getName
  val registry: MetricsRegistry
  val metricGroup = new MetricGroup(group, getPrefix, registry)

  def newCounter(name: String) = metricGroup.newCounter(name)

  def newTimer(name: String) = metricGroup.newTimer(name)

  def newGauge[T](name: String, value: T) = metricGroup.newGauge[T](name,value)

  /**
   * Specify a dynamic gauge that always returns the latest value when polled. 
   * The value closure must be thread safe, since metrics reporters may access 
   * it from another thread.
   */
  def newGauge[T](name: String, value: () => T) = {
    metricGroup.newGauge(name, new ValueFunction[T] {
      override def getValue = value()
    })
  }

  /**
   * Returns a prefix for metric names.
   */
  def getPrefix = ""
}