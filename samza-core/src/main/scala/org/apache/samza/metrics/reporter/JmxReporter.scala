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

package org.apache.samza.metrics.reporter

import java.lang.management.ManagementFactory

import org.apache.samza.util.Logging
import javax.management.MBeanServer
import javax.management.ObjectName

import org.apache.samza.config.Config
import org.apache.samza.metrics._

import scala.collection.JavaConverters._
import org.apache.samza.metrics.JmxUtil._

class JmxReporter(server: MBeanServer) extends MetricsReporter with Logging {
  var sources = Map[ReadableMetricsRegistry, String]()
  var listeners = Map[ReadableMetricsRegistry, ReadableMetricsRegistryListener]()

  def start() {
    for ((registry, listener) <- listeners) {
      // First, add a listener for all new metrics that are added.
      registry.listen(listener)

      // Second, add all existing metrics.
      registry.getGroups.asScala.foreach(group => {
        registry.getGroup(group).asScala.foreach {
          case (name, metric) =>
            metric.visit(new MetricsVisitor {

              def counter(counter: Counter) = registerBean(new JmxCounter(counter, getObjectName(group, name, sources(registry))))

              def gauge[T](gauge: Gauge[T]) = registerBean(new JmxGauge(gauge.asInstanceOf[Gauge[Object]], getObjectName(group, name, sources(registry))))

              def timer(timer: Timer) = registerBean(new JmxTimer(timer, getObjectName(group, name, sources(registry))))

              def listGauge[T](listGauge: ListGauge[T]) = registerBean(new JmxListGauge(listGauge.asInstanceOf[ListGauge[Object]], getObjectName(group, name, sources(registry))))

            })
        }
      })
    }
  }

  def register(source: String, registry: ReadableMetricsRegistry) {
    if (!listeners.contains(registry)) {
      sources += registry -> source
      listeners += registry -> new ReadableMetricsRegistryListener {
        def onCounter(group: String, counter: Counter) {
          registerBean(new JmxCounter(counter, getObjectName(group, counter.getName, source)))
        }

        def onGauge(group: String, gauge: Gauge[_]) {
          registerBean(new JmxGauge(gauge.asInstanceOf[Gauge[Object]], getObjectName(group, gauge.getName, source)))
        }

        def onTimer(group: String, timer: Timer) {
          registerBean(new JmxTimer(timer, getObjectName(group, timer.getName, source)))
        }

        def onListGauge(group: String, listGauge: ListGauge[_]) {
          registerBean(new JmxListGauge(listGauge.asInstanceOf[ListGauge[Object]], getObjectName(group, listGauge.getName, source)))
        }
      }
    } else {
      warn("Trying to re-register a registry for source %s. Ignoring." format source)
    }
  }

  def stop() {
    for ((registry, listener) <- listeners) {
      registry.unlisten(listener)
    }
  }

  def registerBean(bean: MetricMBean) {
    if (!server.isRegistered(bean.objectName)) {
      debug("Registering MBean for %s." format bean.objectName)
      server.registerMBean(bean, bean.objectName)
    }
  }
}

trait MetricMBean {
  def objectName(): ObjectName
}

abstract class AbstractBean(val on: ObjectName) extends MetricMBean {
  override def objectName = on
}

trait JmxGaugeMBean extends MetricMBean {
  def getValue(): Object
}

class JmxGauge(g: org.apache.samza.metrics.Gauge[Object], on: ObjectName) extends JmxGaugeMBean {
  def getValue = g.getValue

  def objectName = on
}

class JmxListGauge(g: org.apache.samza.metrics.ListGauge[Object], on: ObjectName) extends JmxGaugeMBean {
  def getValue = g.getValues

  def objectName = on
}

trait JmxCounterMBean extends MetricMBean {
  def getCount(): Long
}

class JmxCounter(c: org.apache.samza.metrics.Counter, on: ObjectName) extends JmxCounterMBean {
  def getCount() = c.getCount()

  def objectName = on
}

trait JmxTimerMBean extends MetricMBean {
  def getAverageTime(): Double
}

class JmxTimer(t: org.apache.samza.metrics.Timer, on: ObjectName) extends JmxTimerMBean {
  def getAverageTime() = t.getSnapshot().getAverage()

  def objectName = on
}

class JmxReporterFactory extends MetricsReporterFactory with Logging {
  def getMetricsReporter(name: String, containerName: String, config: Config) = {
    info("Creating JMX reporter with  name %s." format name)
    new JmxReporter(ManagementFactory.getPlatformMBeanServer)
  }
}
