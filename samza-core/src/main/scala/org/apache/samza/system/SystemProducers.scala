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

package org.apache.samza.system

import org.apache.samza.serializers.SerdeManager
import grizzled.slf4j.Logging
import org.apache.samza.SamzaException

class SystemProducers(
  producers: Map[String, SystemProducer],
  serdeManager: SerdeManager,
  metrics: SystemProducersMetrics = new SystemProducersMetrics,

  /**
   * If set to true, Samza will drop the messages that have serialization errors
   * and keep running. If set to false, Samza will throw the SamzaException
   * to fail the container. Default is false.
   */
  dropSerializationError: Boolean = false) extends Logging {

  def start {
    debug("Starting producers.")

    producers.values.foreach(_.start)
  }

  def stop {
    debug("Stopping producers.")

    producers.values.foreach(_.stop)
  }

  def register(source: String) {
    debug("Registering source: %s" format source)

    metrics.registerSource(source)

    producers.values.foreach(_.register(source))
  }

  def flush(source: String) {
    debug("Flushing source: %s" format source)

    metrics.flushes.inc
    metrics.sourceFlushes(source).inc

    producers.values.foreach(_.flush(source))
  }

  def send(source: String, envelope: OutgoingMessageEnvelope) {
    trace("Sending message from source: %s, %s" format (envelope, source))

    metrics.sends.inc
    metrics.sourceSends(source).inc

    val bytesEnvelope = try {
      Some(serdeManager.toBytes(envelope))
    } catch {
      case e: Exception if !dropSerializationError => throw new SamzaException("can not serialize the message", e)
      case ex: Throwable => {
        debug("Serialization fails: %s . Drop the error message" format ex)
        metrics.serializationError.inc
        None
      }
    }

    if (!bytesEnvelope.isEmpty) {
      producers(envelope.getSystemStream.getSystem).send(source, bytesEnvelope.get)
    }
  }
}
