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

package org.apache.samza.system.chooser

import java.util.ArrayDeque
import org.apache.samza.config.Config
import org.apache.samza.SamzaException
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.metrics.ReadableMetricsRegistry

import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.metrics.MetricsHelper

/**
 * A chooser that round robins between all system stream partitions. This
 * chooser makes the assumption that it will only ever receive one envelope
 * at a time, per SystemStreamPartition. This is part of the contract between
 * MessageChooser and SystemConsumers. If a second envelope from the a
 * SystemStreamPartition is given to the RoundRobinChooser prior to
 * RoundRobinChooser.choose returning the prior one, a SamzaException will be
 * thrown.
 */
class RoundRobinChooser(metrics: RoundRobinChooserMetrics = new RoundRobinChooserMetrics) extends BaseMessageChooser {

  /**
   * SystemStreamPartitions that the chooser has received a message for, but
   * have not yet returned. Envelopes for these SystemStreamPartitions should
   * be in the queue.
   */
  var inflightSystemStreamPartitions = Set[SystemStreamPartition]()

  /**
   * Queue of potential messages to process. Round robin will always choose
   * the message at the head of the queue. A queue can be used to implement
   * round robin here because we only get one envelope per
   * SystemStreamPartition at a time.
   */
  var q = new ArrayDeque[IncomingMessageEnvelope]()

  override def start {
    metrics.setBufferedMessages(() => q.size)
  }

  def update(envelope: IncomingMessageEnvelope) = {
    if (inflightSystemStreamPartitions.contains(envelope.getSystemStreamPartition)) {
      throw new SamzaException("Received more than one envelope from the same "
        + "SystemStreamPartition without returning the last. This is a "
        + "violation of the contract with SystemConsumers, and breaks this "
        + "RoundRobin implementation.")
    }

    q.add(envelope)
    inflightSystemStreamPartitions += envelope.getSystemStreamPartition
  }

  def choose = {
    val envelope = q.poll

    if (envelope != null) {
      inflightSystemStreamPartitions -= envelope.getSystemStreamPartition
    }

    envelope
  }
}


class RoundRobinChooserMetrics(val registry: MetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {
  def setBufferedMessages(getValue: () => Int) {
    newGauge("buffered-messages", getValue)
  }
}

class RoundRobinChooserFactory extends MessageChooserFactory {
  def getChooser(config: Config, registry: MetricsRegistry) = new RoundRobinChooser(new RoundRobinChooserMetrics(registry))
}