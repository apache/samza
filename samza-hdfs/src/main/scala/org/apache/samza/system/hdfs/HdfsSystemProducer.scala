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

package org.apache.samza.system.hdfs


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.samza.system.hdfs.writer.HdfsWriter
import org.apache.samza.system.{OutgoingMessageEnvelope, SystemProducer}
import org.apache.samza.util.{Logging, TimerUtil}

import scala.collection.mutable.{Map => MMap}


class HdfsSystemProducer(
  systemName: String, clientId: String, config: HdfsConfig, metrics: HdfsSystemProducerMetrics,
  val clock: () => Long = () => System.currentTimeMillis) extends SystemProducer with Logging with TimerUtil {
  val dfs = FileSystem.newInstance(new Configuration(true))
  val writers: MMap[String, HdfsWriter[_]] = MMap.empty[String, HdfsWriter[_]]
  private val lock = new Object //synchronization lock for thread safe access

  def start(): Unit = {
    info("entering HdfsSystemProducer.start() call for system: " + systemName + ", client: " + clientId)
  }

  def stop(): Unit = {
    info("entering HdfsSystemProducer.stop() for system: " + systemName + ", client: " + clientId)

    lock.synchronized {
      writers.values.map(_.close)
      dfs.close
    }
  }

  def register(source: String): Unit = {
    info("entering HdfsSystemProducer.register(" + source + ") " +
      "call for system: " + systemName + ", client: " + clientId)

    lock.synchronized {
      writers += (source -> HdfsWriter.getInstance(dfs, systemName, config))
    }
  }

  def flush(source: String): Unit = {
    debug("entering HdfsSystemProducer.flush(" + source + ") " +
      "call for system: " + systemName + ", client: " + clientId)

    metrics.flushes.inc
    lock.synchronized {
      try {
        updateTimer(metrics.flushMs) {
          writers.get(source).head.flush
        }
      } catch {
        case e: Exception => {
          metrics.flushFailed.inc
          warn("Exception thrown while client " + clientId + " flushed HDFS out stream, msg: " + e.getMessage)
          debug("Detailed message from exception thrown by client " + clientId + " in HDFS flush: ", e)
          writers.get(source).head.close
          throw e
        }
      }
    }
    metrics.flushSuccess.inc
  }

  def send(source: String, ome: OutgoingMessageEnvelope) = {
    debug("entering HdfsSystemProducer.send(source = " + source + ", envelope) " +
      "call for system: " + systemName + ", client: " + clientId)

    metrics.sends.inc
    lock.synchronized {
      try {
        updateTimer(metrics.sendMs) {
          writers.get(source).head.write(ome)
        }
      } catch {
        case e: Exception => {
          metrics.sendFailed.inc
          warn("Exception thrown while client " + clientId + " wrote to HDFS, msg: " + e.getMessage)
          debug("Detailed message from exception thrown by client " + clientId + " in HDFS write: ", e)
          writers.get(source).head.close
          throw e
        }
      }
    }
    metrics.sendSuccess.inc
  }
}