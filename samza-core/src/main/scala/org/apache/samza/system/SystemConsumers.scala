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

import scala.collection.JavaConversions._
import org.apache.samza.serializers.SerdeManager
import org.apache.samza.util.Logging
import org.apache.samza.system.chooser.MessageChooser
import org.apache.samza.SamzaException
import java.util.HashMap
import java.util.ArrayDeque
import java.util.Queue
import java.util.Set
import java.util.HashSet

object SystemConsumers {
  val DEFAULT_POLL_INTERVAL_MS = 50
  val DEFAULT_NO_NEW_MESSAGES_TIMEOUT = 10
  val DEFAULT_DROP_SERIALIZATION_ERROR = false
}

/**
 * The SystemConsumers class coordinates between all SystemConsumers, the
 * MessageChooser, and the SamzaContainer. Its job is to poll each
 * SystemConsumer for messages, update the
 * {@link org.apache.samza.system.chooser.MessageChooser} with new incoming
 * messages, poll the MessageChooser for the next message to process, and
 * return that message to the SamzaContainer.
 */
class SystemConsumers(

  /**
   * The class that determines the order to process incoming messages.
   */
  chooser: MessageChooser,

  /**
   * A map of SystemConsumers that should be polled for new messages.
   */
  consumers: Map[String, SystemConsumer],

  /**
   * The class that handles deserialization of incoming messages.
   */
  serdeManager: SerdeManager = new SerdeManager,

  /**
   * A helper class to hold all of SystemConsumers' metrics.
   */
  metrics: SystemConsumersMetrics = new SystemConsumersMetrics,

  /**
   * If MessageChooser returns null when it's polled, SystemConsumers will
   * poll each SystemConsumer with a timeout next time it tries to poll for
   * messages. Setting the timeout to 0 means that SamzaContainer's main
   * thread will sit in a tight loop polling every SystemConsumer over and
   * over again if no new messages are available.
   */
  noNewMessagesTimeout: Int = SystemConsumers.DEFAULT_NO_NEW_MESSAGES_TIMEOUT,

  /**
   * This parameter is to define how to deal with deserialization failure. If
   * set to true, the task will skip the messages when deserialization fails.
   * If set to false, the task will throw SamzaException and fail the container.
   */
  dropDeserializationError: Boolean = SystemConsumers.DEFAULT_DROP_SERIALIZATION_ERROR,

  /**
   * <p>Defines an upper bound for how long the SystemConsumers will wait
   * before polling systems for more data. The default setting is 50ms, which
   * means that SystemConsumers will poll for new messages for all
   * SystemStreamPartitions with empty buffers every 50ms. SystemConsumers
   * will also poll for new messages any time that there are no available
   * messages to process, or any time the MessageChooser returns a null
   * IncomingMessageEnvelope.</p>
   *
   * <p>This parameter also implicitly defines how much latency is introduced
   * by SystemConsumers. If a message is available for a SystemStreamPartition
   * with no remaining unprocessed messages, the SystemConsumers will poll for
   * it within 50ms of its availability in the stream system.</p>
   */
  pollIntervalMs: Int = SystemConsumers.DEFAULT_POLL_INTERVAL_MS,

  /**
   * Clock can be used to inject a custom clock when mocking this class in
   * tests. The default implementation returns the current system clock time.
   */
  clock: () => Long = () => System.currentTimeMillis) extends Logging {

  /**
   * A buffer of incoming messages grouped by SystemStreamPartition. These
   * messages are handed out to the MessageChooser as it needs them.
   */
  private val unprocessedMessagesBySSP = new HashMap[SystemStreamPartition, Queue[IncomingMessageEnvelope]]()

  /**
   * A set of SystemStreamPartitions grouped by systemName. This is used as a
   * cache to figure out which SystemStreamPartitions we need to poll from the
   * underlying system consumer.
   */
  private val emptySystemStreamPartitionsBySystem = new HashMap[String, Set[SystemStreamPartition]]()

  /**
   * Default timeout to noNewMessagesTimeout. Every time SystemConsumers
   * receives incoming messages, it sets timeout to 0. Every time
   * SystemConsumers receives no new incoming messages from the MessageChooser,
   * it sets timeout to noNewMessagesTimeout again.
   */
  var timeout = noNewMessagesTimeout

  /**
   * The last time that systems were polled for new messages.
   */
  var lastPollMs = 0L

  /**
   * Total number of unprocessed messages in unprocessedMessagesBySSP.
   */
  var totalUnprocessedMessages = 0

  debug("Got stream consumers: %s" format consumers)
  debug("Got no new message timeout: %s" format noNewMessagesTimeout)

  metrics.setTimeout(() => timeout)
  metrics.setNeededByChooser(() => emptySystemStreamPartitionsBySystem.size)
  metrics.setUnprocessedMessages(() => totalUnprocessedMessages)

  def start {
    debug("Starting consumers.")

    emptySystemStreamPartitionsBySystem ++= unprocessedMessagesBySSP
      .keySet
      .groupBy(_.getSystem)
      .mapValues(systemStreamPartitions => new HashSet(systemStreamPartitions.toSeq))

    consumers
      .keySet
      .foreach(metrics.registerSystem)

    consumers
      .values
      .foreach(_.start)

    chooser.start

    refresh
  }

  def stop {
    debug("Stopping consumers.")

    consumers.values.foreach(_.stop)

    chooser.stop
  }

  def register(systemStreamPartition: SystemStreamPartition, offset: String) {
    debug("Registering stream: %s, %s" format (systemStreamPartition, offset))
    metrics.registerSystemStreamPartition(systemStreamPartition)
    unprocessedMessagesBySSP.put(systemStreamPartition, new ArrayDeque[IncomingMessageEnvelope]())
    chooser.register(systemStreamPartition, offset)

    try {
      consumers(systemStreamPartition.getSystem).register(systemStreamPartition, offset)
    } catch {
      case e: NoSuchElementException => throw new SystemConsumersException("can't register " + systemStreamPartition.getSystem + "'s consumer.", e)
    }
  }

  def choose: IncomingMessageEnvelope = {
    val envelopeFromChooser = chooser.choose

    if (envelopeFromChooser == null) {
      trace("Chooser returned null.")

      metrics.choseNull.inc

      // Sleep for a while so we don't poll in a tight loop.
      timeout = noNewMessagesTimeout
    } else {
      val systemStreamPartition = envelopeFromChooser.getSystemStreamPartition

      trace("Chooser returned an incoming message envelope: %s" format envelopeFromChooser)

      // Ok to give the chooser a new message from this stream.
      timeout = 0
      metrics.choseObject.inc
      metrics.systemStreamMessagesChosen(envelopeFromChooser.getSystemStreamPartition).inc

      tryUpdate(systemStreamPartition)
    }

    if (envelopeFromChooser == null || lastPollMs < clock() - pollIntervalMs) {
      refresh
    }

    envelopeFromChooser
  }

  /**
   * Poll all SystemStreamPartitions for which there are currently no new
   * messages to process.
   */
  private def poll(systemName: String) {
    trace("Polling system consumer: %s" format systemName)

    metrics.systemPolls(systemName).inc

    trace("Getting fetch map for system: %s" format systemName)

    val systemFetchSet = emptySystemStreamPartitionsBySystem.get(systemName)

    // Poll when at least one SSP in this system needs more messages.
    if (systemFetchSet.size > 0) {
      val consumer = consumers(systemName)

      trace("Fetching: %s" format systemFetchSet)

      metrics.systemStreamPartitionFetchesPerPoll(systemName).inc(systemFetchSet.size)

      val systemStreamPartitionEnvelopes = consumer.poll(systemFetchSet, timeout)

      trace("Got incoming message envelopes: %s" format systemStreamPartitionEnvelopes)

      metrics.systemMessagesPerPoll(systemName).inc

      val sspAndEnvelopeIterator = systemStreamPartitionEnvelopes.entrySet.iterator

      while (sspAndEnvelopeIterator.hasNext) {
        val sspAndEnvelope = sspAndEnvelopeIterator.next
        val systemStreamPartition = sspAndEnvelope.getKey
        val envelopes = new ArrayDeque(sspAndEnvelope.getValue)
        val numEnvelopes = envelopes.size
        totalUnprocessedMessages += numEnvelopes

        if (numEnvelopes > 0) {
          unprocessedMessagesBySSP.put(systemStreamPartition, envelopes)

          // Update the chooser if it needs a message for this SSP.
          if (emptySystemStreamPartitionsBySystem.get(systemStreamPartition.getSystem).remove(systemStreamPartition)) {
            tryUpdate(systemStreamPartition)
          }
        }
      }
    } else {
      trace("Skipping polling for %s. Already have messages available for all registered SystemStreamPartitions." format (systemName))
    }
  }

  private def tryUpdate(ssp: SystemStreamPartition) {
    var updated = false
    try {
      updated = update(ssp)
    } finally {
      if (!updated) {
        // if failed to update the chooser, add the ssp back into the emptySystemStreamPartitionBySystem map to ensure that we will poll for the next message
        emptySystemStreamPartitionsBySystem.get(ssp.getSystem).add(ssp)
      }
    }
  }

  private def refresh {
    trace("Refreshing chooser with new messages.")

    // Update last poll time so we don't poll too frequently.
    lastPollMs = clock()

    // Poll every system for new messages.
    consumers.keys.map(poll(_))
  }

  /**
   * Tries to update the message chooser with an envelope from the supplied
   * SystemStreamPartition if an envelope is available.
   */
  private def update(systemStreamPartition: SystemStreamPartition) = {
    var updated = false
    val q = unprocessedMessagesBySSP.get(systemStreamPartition)

    while (q.size > 0 && !updated) {
      val rawEnvelope = q.remove
      val deserializedEnvelope = try {
        Some(serdeManager.fromBytes(rawEnvelope))
      } catch {
        case e: Exception if !dropDeserializationError =>
          throw new SystemConsumersException(
            "Cannot deserialize an incoming message for %s"
              .format(systemStreamPartition.getSystemStream.toString), e)
        case ex: Exception =>
          debug("Cannot deserialize an incoming message for %s. Dropping the error message."
                .format(systemStreamPartition.getSystemStream.toString), ex)
          metrics.deserializationError.inc
          None
      }

      if (deserializedEnvelope.isDefined) {
        chooser.update(deserializedEnvelope.get)
        updated = true
      }

      totalUnprocessedMessages -= 1
    }

    updated
  }
}

/**
 * When SystemConsumer registers consumers, there are situations where system can not recover
 * from. Such as a failed consumer is used in task.input and changelogs.
 * SystemConsumersException is thrown to indicate a hard failure when the system can not recover from.
 */
class SystemConsumersException(s: String, t: Throwable) extends SamzaException(s, t) {
  def this(s: String) = this(s, null)
}
