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


import java.util
import java.util.ArrayDeque
import java.util.concurrent.TimeUnit
import java.util.Collections
import java.util.HashMap
import java.util.HashSet
import java.util.Queue
import java.util.Set
import java.util.function.{Consumer}
import java.util.stream.Collectors
import scala.collection.JavaConverters._
import org.apache.samza.serializers.SerdeManager
import org.apache.samza.util.{Logging, TimerUtil}
import org.apache.samza.system.chooser.MessageChooser
import org.apache.samza.SamzaException
import org.apache.samza.config.TaskConfig


object SystemConsumers {
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
class SystemConsumers (

  /**
   * The class that determines the order to process incoming messages.
   */
  chooser: MessageChooser,

  /**
   * A map of SystemConsumers that should be polled for new messages.
   */
  consumers: Map[String, SystemConsumer],

  /**
   * Provides a mapping from system name to a {@see SystemAdmin}.
   */
  systemAdmins: SystemAdmins,

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
   * set to true, the task will notAValidEvent the messages when deserialization fails.
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
  val pollIntervalMs: Int = TaskConfig.DEFAULT_POLL_INTERVAL_MS,

  /**
   * Clock can be used to inject a custom clock when mocking this class in
   * tests. The default implementation returns the current system clock time.
   */
  val clock: () => Long = () => System.nanoTime(),

  val elasticityFactor: Int = 1,

  /**
   * Identifier of the current deployment.
   * */
  val runId: String = null) extends Logging with TimerUtil {

  /**
   * Mapping from the {@see SystemStreamPartition} to the registered offsets.
   */
  private val sspToRegisteredOffsets = new HashMap[SystemStreamPartition, String]()

  /**
   * Set of all the SystemStreamPartitions registered with this SystemConsumers
   * With elasticity-enabled, the SSPs have valid (i.e. >=0) keyBuckets,
   * with elasticity disabled, keyBuckets on all SSPs are = -1.
   */
  private val sspKeyBucketsRegistered = new HashSet[SystemStreamPartition] ()

  private val intermediateSSPs = new HashSet[SystemStreamPartition]()

  private val intermediateSystems = new HashSet[String]()

  /**
   * A buffer of incoming messages grouped by SystemStreamPartition. These
   * messages are handed out to the MessageChooser as it needs them.
   */
  private val unprocessedMessagesBySSP = new HashMap[SystemStreamPartition, Queue[IncomingMessageEnvelope]]()

  /**
   * Set of SSPs that are currently at end-of-stream.
   */
  private val endOfStreamSSPs = new HashSet[SystemStreamPartition]()

  /**
   * A set of SystemStreamPartitions grouped by systemName. This is used as a
   * cache to figure out which SystemStreamPartitions we need to poll from the
   * underlying system consumer.
   */
  private val emptySystemStreamPartitionsBySystem = new HashMap[String, Set[SystemStreamPartition]]()

  /**
    * Denotes if the SystemConsumers have started. The flag is useful in the event of shutting down since interrupt
    * on Samza Container will shutdown components and container currently doesn't track what components have started
    * successfully.
    */
  private var started = false

  @volatile
  private var isDraining = false

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
  var lastPollNs = 0L

  /**
   * Total number of unprocessed messages in unprocessedMessagesBySSP.
   */
  var totalUnprocessedMessages = 0

  info("Got elasticity factor: %s" format elasticityFactor)
  debug("Got stream consumers: %s" format consumers)
  debug("Got no new message timeout: %s" format noNewMessagesTimeout)

  metrics.setTimeout(() => timeout)
  metrics.setNeededByChooser(() => emptySystemStreamPartitionsBySystem.size)
  metrics.setUnprocessedMessages(() => totalUnprocessedMessages)

  def start {
    for ((systemStreamPartition, offset) <- sspToRegisteredOffsets.asScala) {
      val consumer = consumers(systemStreamPartition.getSystem)
      // If elasticity is enabled then the RunLoop gives SSP with keybucket
      // but the actual systemConsumer which consumes from the input does not know about KeyBucket.
      // hence, use an SSP without KeyBucket
      consumer.register(removeKeyBucket(systemStreamPartition), offset)
    }

    debug("Starting consumers.")
    emptySystemStreamPartitionsBySystem.asScala ++= unprocessedMessagesBySSP
      .keySet
      .asScala
      .groupBy(_.getSystem)
      .mapValues(systemStreamPartitions => new util.HashSet(systemStreamPartitions.toSeq.asJava))

    consumers
      .keySet
      .foreach(metrics.registerSystem)

    consumers
      .values
      .foreach(_.start)

    chooser.start

    // SystemConsumers could be set to drain mode prior to start if a drain message was encountered on container start
    if (isDraining) {
      writeDrainControlMessageToSspQueue()
    }

    started = true

    refresh
  }

  def stop {
    if (started) {
      debug("Stopping consumers.")

      consumers.values.foreach(_.stop)

      chooser.stop

      started = false
    } else {
      debug("Ignoring the consumers stop request since it never started.")
    }
  }

  def drain(): Unit = {
    if (!isDraining) {
      isDraining = true;
      info("SystemConsumers is set to drain mode.")
      if (started) {
        writeDrainControlMessageToSspQueue()
      }
    }
  }

  def register(ssp: SystemStreamPartition, offset: String) {
    // If elasticity is enabled then the RunLoop gives SSP with keybucket
    // but the MessageChooser does not know about the KeyBucket
    // hence, use an SSP without KeyBucket
    sspKeyBucketsRegistered.add(ssp)
    val systemStreamPartition = removeKeyBucket(ssp)
    debug("Registering stream: %s, %s" format (systemStreamPartition, offset))

    if (IncomingMessageEnvelope.END_OF_STREAM_OFFSET.equals(offset)) {
      info("Stream : %s is already at end of stream" format (systemStreamPartition))
      endOfStreamSSPs.add(systemStreamPartition)
      return
    }

    metrics.registerSystemStreamPartition(systemStreamPartition)
    unprocessedMessagesBySSP.put(systemStreamPartition, new ArrayDeque[IncomingMessageEnvelope]())

    chooser.register(systemStreamPartition, offset)

    try {
      val consumer = consumers(systemStreamPartition.getSystem)
      val existingOffset = sspToRegisteredOffsets.get(systemStreamPartition)
      val systemAdmin = systemAdmins.getSystemAdmin(systemStreamPartition.getSystem)
      val offsetComparisonResult = systemAdmin.offsetComparator(existingOffset, offset)
      if (existingOffset == null || (offsetComparisonResult != null && offsetComparisonResult > 0)) {
        sspToRegisteredOffsets.put(systemStreamPartition, offset)
      }
    } catch {
      case e: NoSuchElementException => throw new SystemConsumersException("can't register " + systemStreamPartition.getSystem + "'s consumer.", e)
    }
  }

  def registerIntermediateSSP(ssp: SystemStreamPartition): Unit = {
    debug("Registering intermediate stream: %s" format ssp)
    intermediateSSPs.add(ssp)
    intermediateSystems.add(ssp.getSystem)
  }

  def isEndOfStream(systemStreamPartition: SystemStreamPartition) = {
    endOfStreamSSPs.contains(removeKeyBucket(systemStreamPartition))
  }

  def choose(updateChooser: Boolean = true): IncomingMessageEnvelope = {
    val envelopeFromChooser = chooser.choose

    updateTimer(metrics.deserializationNs) {
      if (envelopeFromChooser == null) {
        trace("Chooser returned null.")

        metrics.choseNull.inc

        // Sleep for a while so we don't poll in a tight loop, but, don't do this when called from the RunLoop
        // code because in that case the chooser will not get updated with a new message for an SSP until after a
        // message is processed, See how updateChooser variable is used below. The RunLoop has its own way to
        // block when there is no work to process.
        timeout = if (updateChooser) noNewMessagesTimeout else 0
      } else {
        val systemStreamPartition = envelopeFromChooser.getSystemStreamPartition

        if (envelopeFromChooser.isEndOfStream) {
          info("End of stream reached for partition: %s" format systemStreamPartition)
          endOfStreamSSPs.add(systemStreamPartition)
        }

        trace("Chooser returned an incoming message envelope: %s" format envelopeFromChooser)

        // Ok to give the chooser a new message from this stream.
        timeout = 0
        if (elasticityFactor == 1) {
          metrics.choseObject.inc
          metrics.systemStreamMessagesChosen(envelopeFromChooser.getSystemStreamPartition).inc
        } else {
          // increment metrics only if the envelope belongs to one of the SSP key buckets registered with this SystemConsumers
          if (sspKeyBucketsRegistered.contains(envelopeFromChooser.getSystemStreamPartition(elasticityFactor))) {
            metrics.choseObject.inc
            metrics.systemStreamMessagesChosen(envelopeFromChooser.getSystemStreamPartition).inc
          } else {
            metrics.choseNull.inc
          }
        }
        if (updateChooser) {
          trace("Update chooser for " + systemStreamPartition.getPartition)
          tryUpdate(systemStreamPartition)
        }
      }
    }

    updateTimer(metrics.pollNs) {
      if (envelopeFromChooser == null || TimeUnit.NANOSECONDS.toMillis(clock() - lastPollNs) > pollIntervalMs) {
        refresh
      }
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

    val systemFetchSet : util.Set[SystemStreamPartition] =
      if (emptySystemStreamPartitionsBySystem.containsKey(systemName)) {
        val sspToFetch = new util.HashSet(emptySystemStreamPartitionsBySystem.get(systemName))
        sspToFetch.removeAll(endOfStreamSSPs)
        sspToFetch
      } else {
        Collections.emptySet()
      }

    // Poll when at least one SSP in this system needs more messages.

    if (systemFetchSet != null && systemFetchSet.size > 0) {
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
      trace("Skipping polling for %s. Already have messages available for all registered SystemStreamPartitions." format systemName)
    }
  }

  def tryUpdate(ssp: SystemStreamPartition) {
    val systemStreamPartition = removeKeyBucket(ssp)
    var updated = false
    try {
      updated = update(systemStreamPartition)
    } finally {
      if (!updated) {
        // if failed to update the chooser, add the ssp back into the emptySystemStreamPartitionBySystem map to ensure that we will poll for the next message
        emptySystemStreamPartitionsBySystem.get(systemStreamPartition.getSystem).add(systemStreamPartition)
      }
    }
  }

  private def refresh {
    // Update last poll time so we don't poll too frequently.
    lastPollNs = clock()

    if (isDraining) {
      trace("Refreshing chooser with new messages from intermediate systems.")

      // scala 2.11 doesn't allow using syntactical sugar: intermediateSystems.foreach(poll(_)) over java collections
      intermediateSystems.forEach(new Consumer[String] {
        override def accept(system: String): Unit = poll(system)
      })
    } else {
      trace("Refreshing chooser with new messages.")
      consumers.keys.foreach(poll(_))
    }
  }

  private def writeDrainControlMessageToSspQueue() {
    val sspsToDrain = new HashSet(sspKeyBucketsRegistered)

    // only write Drain ControlMessages to source SSPs
    // sspsToDrain = allSSPs - intermediateSSPs - eosSSPs
    sspsToDrain.removeAll(intermediateSSPs)
    sspsToDrain.removeAll(endOfStreamSSPs)

    sspsToDrain.forEach(new Consumer[SystemStreamPartition] {
      override def accept(ssp: SystemStreamPartition): Unit = {
        val envelopes: Queue[IncomingMessageEnvelope] =
          if (unprocessedMessagesBySSP.containsKey(ssp)) {
            unprocessedMessagesBySSP.get(ssp)
          } else {
            new util.ArrayDeque[IncomingMessageEnvelope]()
          }

        // Add watermark ControlMessage only if there are intermediate SSPs as low-level API task doesn't process
        // WatermarkMessages
        if (!intermediateSSPs.isEmpty) {
          envelopes.add(IncomingMessageEnvelope.buildWatermarkEnvelope(ssp, Long.MaxValue))
          totalUnprocessedMessages += 1
        }
        // Add Drain ControlMessage
        envelopes.add(IncomingMessageEnvelope.buildDrainMessage(ssp, runId))
        totalUnprocessedMessages += 1
        unprocessedMessagesBySSP.put(ssp, envelopes)

        // update the chooser with the messages
        tryUpdate(ssp)
      }
    })
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
        case e: Throwable if !dropDeserializationError =>
          throw new SystemConsumersException(
            "Cannot deserialize an incoming message for %s"
              .format(systemStreamPartition.getSystemStream.toString), e)
        case ex: Throwable =>
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

  private def removeKeyBucket(sspWithKeyBucket: SystemStreamPartition): SystemStreamPartition = {
    new SystemStreamPartition(sspWithKeyBucket.getSystem, sspWithKeyBucket.getStream, sspWithKeyBucket.getPartition)
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
