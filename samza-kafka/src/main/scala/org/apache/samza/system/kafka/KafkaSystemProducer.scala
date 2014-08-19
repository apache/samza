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

package org.apache.samza.system.kafka

import scala.collection.mutable.ArrayBuffer
import grizzled.slf4j.Logging
import kafka.producer.KeyedMessage
import kafka.producer.Producer
import org.apache.samza.system.SystemProducer
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.util.ExponentialSleepStrategy
import org.apache.samza.util.TimerUtils

class KafkaSystemProducer(
  systemName: String,
  batchSize: Int,
  retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy,
  getProducer: () => Producer[Object, Object],
  metrics: KafkaSystemProducerMetrics,
  val clock: () => Long = () => System.currentTimeMillis) extends SystemProducer with Logging with TimerUtils {

  var sourceBuffers = Map[String, ArrayBuffer[KeyedMessage[Object, Object]]]()
  var producer: Producer[Object, Object] = null

  def start() {
  }

  def stop() {
    if (producer != null) {
      producer.close
    }
  }

  def register(source: String) {
    sourceBuffers += source -> ArrayBuffer()

    metrics.setBufferSize(source, () => sourceBuffers(source).size)
  }

  def send(source: String, envelope: OutgoingMessageEnvelope) {
    trace("Enqueueing message: %s, %s." format (source, envelope))

    metrics.sends.inc

    sourceBuffers(source) += new KeyedMessage[Object, Object](
      envelope.getSystemStream.getStream,
      envelope.getKey,
      envelope.getPartitionKey,
      envelope.getMessage)

    if (sourceBuffers(source).size >= batchSize) {
      flush(source)
    }
  }

  def flush(source: String) {
    updateTimer(metrics.flushMs) {
      val buffer = sourceBuffers(source)
      trace("Flushing buffer with size: %s." format buffer.size)
      metrics.flushes.inc

      retryBackoff.run(
        loop => {
          if (producer == null) {
            info("Creating a new producer for system %s." format systemName)
            producer = getProducer()
            debug("Created a new producer for system %s." format systemName)
          }

          producer.send(buffer: _*)
          loop.done
          metrics.flushSizes.inc(buffer.size)
        },

        (exception, loop) => {
          warn("Triggering a reconnect for %s because connection failed: %s" format (systemName, exception))
          debug("Exception detail: ", exception)
          metrics.reconnects.inc

          if (producer != null) {
            producer.close
            producer = null
          }
        })

      buffer.clear
      trace("Flushed buffer.")
    }
  }
}
