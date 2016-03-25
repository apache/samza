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
package org.apache.samza.coordinator

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.samza.metrics.{Gauge, MetricsRegistryMap}
import org.apache.samza.system.{StreamMetadataCache, SystemStream, SystemStreamMetadata}
import org.apache.samza.util.Logging

private[coordinator] class StreamPartitionCountMonitor (
  val streamsToMonitor: Set[SystemStream],
  val metadataCache: StreamMetadataCache,
  val metrics: MetricsRegistryMap,
  val monitorFrequency: Int = 300000) extends Logging {

  val initialMetadata: Map[SystemStream, SystemStreamMetadata] = metadataCache.getStreamMetadata(streamsToMonitor, true)
  val gauges = new java.util.HashMap[SystemStream, Gauge[Int]]()
  private val running: AtomicBoolean = new AtomicBoolean(false)
  private var thread: Thread = null
  private val lock = new Object

  private def getMonitorThread(): Thread = {
    new Thread(new Runnable {
      override def run(): Unit = {
        while (running.get()) {
          try {
            var currentMetadata: Map[SystemStream, SystemStreamMetadata] = Map[SystemStream, SystemStreamMetadata]()
            currentMetadata = metadataCache.getStreamMetadata(streamsToMonitor, true)
            initialMetadata.map {
              case (systemStream, metadata) => {
                val currentPartitionCount = currentMetadata(systemStream).getSystemStreamPartitionMetadata.keySet().size()
                val prevPartitionCount = metadata.getSystemStreamPartitionMetadata.keySet().size()

                val gauge = if (gauges.containsKey(systemStream)) {
                  gauges.get(systemStream)
                } else {
                  metrics.newGauge[Int](
                    "job-coordinator",
                    String.format("%s-%s-partitionCount", systemStream.getSystem, systemStream.getStream),
                    0)
                }
                gauge.set(currentPartitionCount - prevPartitionCount)
                gauges.put(systemStream, gauge)
              }
            }
            lock synchronized {
              lock.wait(monitorFrequency)
            }
          } catch {
            case ie: InterruptedException =>
              info("Received Interrupted Exception: %s" format ie, ie)
            case e: Exception =>
              warn("Received Exception: %s" format e, e)
          }
        }
      }
    })
  }

  def startMonitor(): Unit = {
    if (thread == null || !thread.isAlive) {
      thread = getMonitorThread()
      running.set(true)
      thread.start()
    }
  }

  /**
   * Used in unit tests only
   * @return Returns true if the monitor thread is running and false, otherwise
   */
  def isRunning(): Boolean = {
    thread != null && thread.isAlive
  }

  def stopMonitor(): Unit = {
    try {
      running.set(false)
      lock synchronized  {
        lock.notify()
      }
      if (thread != null) {
        thread.join(monitorFrequency)
      }
    } catch {
      case e: Exception =>
        println("[STOP MONITOR] Received Exception: %s" format e)
        e.printStackTrace()
    }
  }

}
