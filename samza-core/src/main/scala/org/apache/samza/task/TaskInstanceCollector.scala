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

package org.apache.samza.task

import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.SystemProducers
import org.apache.samza.container.TaskInstanceMetrics
import org.apache.samza.util.Logging

/**
 * TaskInstanceCollector is an implementation of MessageCollector that sends
 * messages to the underlying SystemProducers class immediately. Since the
 * SystemProducers object is typically shared between TaskInstances (in order
 * to share connections to the underlying system), this class is necessary as
 * a way to fill in the "source" for the underlying SystemProducers. If a
 * StreamTask calls collector.send, the messages will be immediately given to
 * the SystemProducers, which will in turn forward the outgoing message
 * immediately to the underlying producer for the system. Note that, if the
 * underlying system producer buffers messages, then using this collector will
 * still not result in an immediate send, but calling flush on it should.
 */
class TaskInstanceCollector(
  producerMultiplexer: SystemProducers,
  metrics: TaskInstanceMetrics = new TaskInstanceMetrics) extends MessageCollector with Logging {

  /**
   * Register as a new source with SystemProducers. This allows this collector
   * to send messages to the SystemProducers.
   */
  def register {
    debug("Registering source: %s" format metrics.source)
    producerMultiplexer.register(metrics.source)
  }

  /**
   * Sends a message to the underlying SystemProducers.
   *
   * @param envelope An outgoing envelope that's to be sent to SystemProducers.
   */
  def send(envelope: OutgoingMessageEnvelope) {
    trace("Sending message from source: %s, %s" format (metrics.source, envelope))
    metrics.sends.inc
    metrics.messagesSent.inc
    producerMultiplexer.send(metrics.source, envelope)
  }

  /**
   * Flushes the underlying SystemProducers.
   */
  def flush {
    trace("Flushing messages from source: %s" format metrics.source)
    metrics.flushes.inc
    producerMultiplexer.flush(metrics.source)
  }
}