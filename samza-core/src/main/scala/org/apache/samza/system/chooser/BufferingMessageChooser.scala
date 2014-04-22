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

import scala.collection.mutable.Queue
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.IncomingMessageEnvelope

/**
 * This buffer is responsible for storing new unprocessed
 * IncomingMessageEnvelopes, feeding the envelopes to the message chooser,
 * and coordinating with the chooser to pick the next message to be processed.
 */
class BufferingMessageChooser(chooser: MessageChooser) extends MessageChooser {
  import ChooserStatus._

  /**
   * A buffer of incoming messages grouped by SystemStreamPartition.
   */
  var unprocessedMessages = Map[SystemStreamPartition, Queue[IncomingMessageEnvelope]]()

  /**
   * A count of all messages sitting in SystemConsumers' unprocessedMessages
   * buffer, and inside the MessageChooser.
   */
  var totalUnprocessedMessages = 0

  /**
   * A map that contains the current status for each SSP that's been
   * registered to the coordinator.
   */
  var statuses = Map[SystemStreamPartition, ChooserStatus]()

  /**
   * This is a cache of all SSPs that are currently in the "NeededByChooser"
   * state. It's simply here to improve performance, since it means we don't
   * need to iterate over all SSPs in the statuses map in order to determine
   * which SSPs are currently needed by the chooser.
   */
  var neededByChooser = Set[SystemStreamPartition]()

  /**
   * Start the chooser that this buffer is managing.
   */
  def start = chooser.start

  /**
   * Stop the chooser that this buffer is managing.
   */
  def stop = chooser.stop

  /**
   * Register a new SystemStreamPartition with this buffer, as well as the
   * underlying chooser.
   */
  def register(systemStreamPartition: SystemStreamPartition, offset: String) {
    unprocessedMessages += systemStreamPartition -> Queue[IncomingMessageEnvelope]()
    chooser.register(systemStreamPartition, offset)
    setStatus(systemStreamPartition, NeededByChooser)
  }

  /**
   * Add a new unprocessed IncomingMessageEnvelope to the buffer. The buffer
   * will hold on to this envelope, and eventually feed it to the underlying chooser.
   */
  def update(envelope: IncomingMessageEnvelope) {
    val systemStreamPartition = envelope.getSystemStreamPartition

    unprocessedMessages(envelope.getSystemStreamPartition).enqueue(envelope)
    totalUnprocessedMessages += 1

    if (statuses(systemStreamPartition).equals(SkippingChooser)) {
      // If we were skipping messages from this SystemStreamPartition, update 
      // neededByChooser since we've got messages for it now.
      setStatus(systemStreamPartition, NeededByChooser)
    }
  }

  /**
   * Tell the buffer to update the underlying chooser with any SSPs that the
   * chooser needs.
   *
   * @return A set of SystemStreamPartitions that were updated in the chooser
   *         as a result of this method invocation.
   */
  def flush: Set[SystemStreamPartition] = {
    var updatedSystemStreamPartitions = Set[SystemStreamPartition]()

    neededByChooser.foreach(systemStreamPartition => {
      if (unprocessedMessages(systemStreamPartition).size > 0) {
        // If we have messages for a stream that the chooser needs, then update.
        chooser.update(unprocessedMessages(systemStreamPartition).dequeue)
        updatedSystemStreamPartitions += systemStreamPartition
        setStatus(systemStreamPartition, InChooser)
      } else {
        // If we find that we have no messages for this SystemStreamPartition, 
        // rather than continue trying to update the chooser with this 
        // SystemStreamPartition, add it to the skip set and remove it from 
        // the neededByChooser set (see below).
        setStatus(systemStreamPartition, SkippingChooser)
      }
    })

    updatedSystemStreamPartitions
  }

  /**
   * Choose a message from the underlying chooser, and return it.
   *
   * @return The IncomingMessageEnvelope that the chooser has picked, or null
   *         if the chooser didn't pick anything.
   */
  def choose = {
    val envelope = chooser.choose

    if (envelope != null) {
      setStatus(envelope.getSystemStreamPartition, NeededByChooser)

      // Chooser picked a message, so we've got one less unprocessed message.
      totalUnprocessedMessages -= 1
    }

    envelope
  }

  /**
   * Update the status of a SystemStreamPartition.
   */
  private def setStatus(systemStreamPartition: SystemStreamPartition, status: ChooserStatus) {
    statuses += systemStreamPartition -> status

    if (status.equals(NeededByChooser)) {
      neededByChooser += systemStreamPartition
    } else {
      neededByChooser -= systemStreamPartition
    }
  }
}

/**
 * ChooserStatus denotes the current state of a SystemStreamPartition for a
 * MessageChooser. This state is used to improve performance in the buffer.
 * To update a MessageChooser, we first check if an envelope exists for each
 * SSP that the buffer needs. If the buffer contains a lot of empty queues,
 * then the operation of iterating over all needed SSPs, and discovering that
 * their queues are empty is a waste of time, since they'll remain empty until
 * a new envelope is added to the queue, which the buffer knows by virtue of
 * having access to the enqueue method. Instead, we stop checking for an empty
 * SSP (SkippingChooser), until a new envelope is added via the enqueue method.
 */
object ChooserStatus extends Enumeration {
  type ChooserStatus = Value

  /**
   * When an envelope has been updated for the MessageChooser, the
   * SystemStreamPartition for the envelope should be set to the InChooser
   * state. The state will remain this way until the MessageChooser returns an
   * envelope with the same SystemStreamPartition, at which point, the
   * SystemStreamPartition's state should be transitioned to NeededByChooser
   * (see below).
   */
  val InChooser = Value

  /**
   * If a SystemStreamPartition is not in the InChooser state, and it's
   * unclear if the buffer has more messages available for the SSP, the SSP
   * should be in the NeededByChooser state. This state means that the chooser
   * should be updated with a new message from the SSP, if one is available.
   */
  val NeededByChooser = Value

  /**
   * When a SystemStreamPartition is in the NeededByChooser state, and we try
   * to update the message chooser with a new envelope from the buffer for the
   * SSP, there are two potential outcomes. One is that there is an envelope
   * in the buffer for the SSP. In this case, the state will be transitioned
   * to InChooser. The other possibility is that there is no envelope in the
   * buffer at the time the update is trying to occur. In the latter case, the
   * SSP is transitioned to the SkippingChooser state. This state means that
   * the buffer will cease to update the chooser with envelopes from this SSP
   * until a new envelope for the SSP is added to the buffer again.
   *
   * <br/><br/>
   *
   * The reason that this state exists is purely to improve performance. If we
   * simply leave SSPs in the NeededByChooser state, we will try and update the
   * chooser on every updateChooser call for all SSPs. This is a waste of time
   * if the buffer contains a large number of empty SSPs.
   */
  val SkippingChooser = Value
}
