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

import java.util.HashMap
import java.util.Map
import scala.collection.JavaConversions._
import org.apache.samza.util.Logging
import org.apache.samza.metrics.Counter
import org.apache.samza.metrics.Gauge
import org.apache.samza.metrics.Timer
import org.apache.samza.metrics.MetricsReporter
import org.apache.samza.metrics.MetricsVisitor
import org.apache.samza.metrics.ReadableMetricsRegistry
import java.util.concurrent.Executors
import org.apache.samza.util.DaemonThreadFactory
import java.util.concurrent.TimeUnit
import org.apache.samza.serializers.Serializer
import org.apache.samza.system.SystemProducer
import org.apache.samza.system.SystemStream
import org.apache.samza.system.OutgoingMessageEnvelope

/**
 *  Companion object for class MetricsSnapshotReporter encapsulating various constants
 */
object MetricsSnapshotReporter {
  val METRIC_SNAPSHOT_REPORTER_THREAD_NAME_PREFIX = "METRIC-SNAPSHOT-REPORTER"
}

/**
 * MetricsSnapshotReporter is a generic metrics reporter that sends metrics to a stream.
 *
 * jobName // my-samza-job
 * jobId // an id that differentiates multiple executions of the same job
 * taskName // container_567890
 * host // eat1-app128.gird
 * version // 0.0.1
 */
class MetricsSnapshotReporter(
  producer: SystemProducer,
  out: SystemStream,
  pollingInterval: Int,
  jobName: String,
  jobId: String,
  containerName: String,
  version: String,
  samzaVersion: String,
  host: String,
  serializer: Serializer[MetricsSnapshot] = null,
  clock: () => Long = () => { System.currentTimeMillis }) extends MetricsReporter with Runnable with Logging {

  val executor = Executors.newScheduledThreadPool(1, new DaemonThreadFactory(MetricsSnapshotReporter.METRIC_SNAPSHOT_REPORTER_THREAD_NAME_PREFIX))
  val resetTime = clock()
  var registries = List[(String, ReadableMetricsRegistry)]()

  info("got metrics snapshot reporter properties [job name: %s, job id: %s, containerName: %s, version: %s, samzaVersion: %s, host: %s, pollingInterval %s]"
    format (jobName, jobId, containerName, version, samzaVersion, host, pollingInterval))

  def start {
    info("Starting producer.")

    producer.start

    info("Starting reporter timer.")

    executor.scheduleWithFixedDelay(this, 0, pollingInterval, TimeUnit.SECONDS)
  }

  def register(source: String, registry: ReadableMetricsRegistry) {
    registries ::= (source, registry)

    info("Registering %s with producer." format source)

    producer.register(source)
  }

  def stop = {
    info("Stopping producer.")

    producer.stop

    info("Stopping reporter timer.")

    executor.shutdown
    executor.awaitTermination(60, TimeUnit.SECONDS)

    if (!executor.isTerminated) {
      warn("Unable to shutdown reporter timer.")
    }
  }

  def run {
    debug("Begin flushing metrics.")

    for ((source, registry) <- registries) {
      debug("Flushing metrics for %s." format source)

      val metricsMsg = new HashMap[String, Map[String, Object]]

      // metrics
      registry.getGroups.foreach(group => {
        val groupMsg = new HashMap[String, Object]

        registry.getGroup(group).foreach {
          case (name, metric) =>
            metric.visit(new MetricsVisitor {
              def counter(counter: Counter) = groupMsg.put(name, counter.getCount: java.lang.Long)
              def gauge[T](gauge: Gauge[T]) = groupMsg.put(name, gauge.getValue.asInstanceOf[Object])
              def timer(timer: Timer) = groupMsg.put(name, timer.getSnapshot().getAverage(): java.lang.Double)
            })
        }

        metricsMsg.put(group, groupMsg)
      })

      val header = new MetricsHeader(jobName, jobId, containerName, source, version, samzaVersion, host, clock(), resetTime)
      val metrics = new Metrics(metricsMsg)

      debug("Flushing metrics for %s to %s with header and map: header=%s, map=%s." format (source, out, header.getAsMap, metrics.getAsMap))

      val metricsSnapshot = new MetricsSnapshot(header, metrics)
      val maybeSerialized = if (serializer != null) {
        serializer.toBytes(metricsSnapshot)
      } else {
        metricsSnapshot
      }

      producer.send(source, new OutgoingMessageEnvelope(out, host, null, maybeSerialized))

      // Always flush, since we don't want metrics to get batched up.
      producer.flush(source)
    }

    debug("Finished flushing metrics.")
  }
}
