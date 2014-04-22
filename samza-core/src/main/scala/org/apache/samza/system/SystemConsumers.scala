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
import scala.collection.mutable.Queue
import org.apache.samza.serializers.SerdeManager
import grizzled.slf4j.Logging
import org.apache.samza.system.chooser.MessageChooser
import org.apache.samza.util.DoublingBackOff
import org.apache.samza.system.chooser.BufferingMessageChooser

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
   * The maximum number of messages to poll from a single SystemStreamPartition.
   */
  maxMsgsPerStreamPartition: Int = 10000,

  /**
   * If MessageChooser returns null when it's polled, SystemConsumers will
   * poll each SystemConsumer with a timeout next time it tries to poll for
   * messages. Setting the timeout to 0 means that SamzaContainer's main
   * thread will sit in a tight loop polling every SystemConsumer over and
   * over again if no new messages are available.
   */
  noNewMessagesTimeout: Long = 10) extends Logging {

  /**
   * The buffer where SystemConsumers stores all incoming message envelopes.
   */
  val buffer = new BufferingMessageChooser(chooser)

  /**
   * A map of every SystemStreamPartition that SystemConsumers is responsible
   * for polling. The values are how many messages to poll for during the next
   * SystemConsumers.poll call.
   *
   * If the value for a SystemStreamPartition is maxMsgsPerStreamPartition,
   * then the implication is that SystemConsumers has no incoming messages in
   * its buffer for the SystemStreamPartition. If the value is 0 then the
   * SystemConsumers' buffer is full for the SystemStreamPartition.
   */
  var fetchMap = Map[SystemStreamPartition, java.lang.Integer]()

  /**
   * A cache of fetchMap values, grouped according to the system. This is
   * purely a trick to get better performance out of the SystemConsumsers
   * class, since the map from systemName to its fetchMap is used for every
   * poll call.
   */
  var systemFetchMapCache = Map[String, Map[SystemStreamPartition, java.lang.Integer]]()

  /**
   * Default timeout to noNewMessagesTimeout. Every time SystemConsumers
   * receives incoming messages, it sets timout to 0. Every time
   * SystemConsumers receives no new incoming messages from the MessageChooser,
   * it sets timeout to noNewMessagesTimeout again.
   */
  var timeout = noNewMessagesTimeout

  /**
   * Make the maximum backoff proportional to the number of streams we're consuming.
   * For a few streams, make the max back off 1, but for hundreds make it up to 1k,
   * which experimentally has shown to be the most performant.
   */
  var maxBackOff = 0

  /**
   * How low totalUnprocessedMessages has to get before the consumers are
   * polled for more data. This is defined to be 10% of
   * maxMsgsPerStreamPartition. Since maxMsgsPerStreamPartition defaults to
   * 10000, the default refreshThreshold is 1000.
   */
  val refreshThreshold = maxMsgsPerStreamPartition * .1

  debug("Got stream consumers: %s" format consumers)
  debug("Got max messages per stream: %s" format maxMsgsPerStreamPartition)
  debug("Got no new message timeout: %s" format noNewMessagesTimeout)

  metrics.setUnprocessedMessages(() => buffer.unprocessedMessages.size)
  metrics.setNeededByChooser(() => buffer.neededByChooser.size)
  metrics.setTimeout(() => timeout)
  metrics.setMaxMessagesPerStreamPartition(() => maxMsgsPerStreamPartition)
  metrics.setNoNewMessagesTimeout(() => noNewMessagesTimeout)

  def start {
    debug("Starting consumers.")

    maxBackOff = scala.math.pow(10, scala.math.log10(fetchMap.size).toInt).toInt

    debug("Got maxBackOff: " + maxBackOff)

    consumers
      .keySet
      .foreach(metrics.registerSystem)

    consumers.values.foreach(_.start)

    buffer.start
  }

  def stop {
    debug("Stopping consumers.")

    consumers.values.foreach(_.stop)

    buffer.stop
  }

  def register(systemStreamPartition: SystemStreamPartition, offset: String) {
    debug("Registering stream: %s, %s" format (systemStreamPartition, offset))

    metrics.registerSystemStream(systemStreamPartition.getSystemStream)
    buffer.register(systemStreamPartition, offset)
    updateFetchMap(systemStreamPartition, maxMsgsPerStreamPartition)
    consumers(systemStreamPartition.getSystem).register(systemStreamPartition, offset)
  }

  /**
   * Needs to be be lazy so that we are sure to get the value of maxBackOff assigned
   * in start(), rather than its initial value.
   */
  lazy val refresh = new DoublingBackOff(maxBackOff) {
    def call(): Boolean = {
      debug("Refreshing chooser with new messages.")

      // Poll every system for new messages.
      consumers.keys.map(poll(_)).contains(true)
    }
  }

  def choose: IncomingMessageEnvelope = {
    val envelopeFromChooser = buffer.choose

    if (envelopeFromChooser == null) {
      debug("Chooser returned null.")

      metrics.choseNull.inc

      // Allow blocking if the chooser didn't choose a message.
      timeout = noNewMessagesTimeout
    } else {
      debug("Chooser returned an incoming message envelope: %s" format envelopeFromChooser)

      metrics.choseObject.inc

      // Don't block if we have a message to process.
      timeout = 0

      metrics.systemStreamMessagesChosen(envelopeFromChooser.getSystemStreamPartition.getSystemStream).inc
    }

    // Always refresh if we got nothing from the chooser. Otherwise, just 
    // refresh when the buffer is low. 
    if (envelopeFromChooser == null || buffer.totalUnprocessedMessages <= refreshThreshold) {
      refresh.maybeCall()
    }

    updateMessageChooser
    envelopeFromChooser
  }

  /**
   * Poll a system for new messages from SystemStreamPartitions that have
   * dipped below the depletedQueueSizeThreshold threshold.  Return true if
   * any envelopes were found, false if none.
   */
  private def poll(systemName: String): Boolean = {
    debug("Polling system consumer: %s" format systemName)

    metrics.systemPolls(systemName).inc

    val consumer = consumers(systemName)

    debug("Getting fetch map for system: %s" format systemName)

    val systemFetchMap = systemFetchMapCache(systemName)

    debug("Fetching: %s" format systemFetchMap)

    metrics.systemStreamPartitionFetchesPerPoll(systemName).inc(systemFetchMap.size)

    val incomingEnvelopes = consumer.poll(systemFetchMap, timeout)

    debug("Got incoming message envelopes: %s" format incomingEnvelopes)

    metrics.systemMessagesPerPoll(systemName).inc

    // We have new un-processed envelopes, so update maps accordingly.
    incomingEnvelopes.foreach(envelope => {
      val systemStreamPartition = envelope.getSystemStreamPartition

      buffer.update(serdeManager.fromBytes(envelope))

      debug("Got message for: %s, %s" format (systemStreamPartition, envelope))

      updateFetchMap(systemStreamPartition, -1)

      debug("Updated fetch map for: %s, %s" format (systemStreamPartition, fetchMap))
    })

    !incomingEnvelopes.isEmpty
  }

  /**
   * A helper method that updates both fetchMap and systemFetchMapCache
   * simultaneously. This is a convenience method to make sure that the
   * systemFetchMapCache stays in sync with fetchMap.
   */
  private def updateFetchMap(systemStreamPartition: SystemStreamPartition, amount: Int = 1) {
    val fetchSize = fetchMap.getOrElse(systemStreamPartition, java.lang.Integer.valueOf(0)).intValue + amount
    val systemName = systemStreamPartition.getSystem
    var systemFetchMap = systemFetchMapCache.getOrElse(systemName, Map())

    if (fetchSize >= refreshThreshold) {
      systemFetchMap += systemStreamPartition -> fetchSize
    } else {
      systemFetchMap -= systemStreamPartition
    }

    fetchMap += systemStreamPartition -> fetchSize
    systemFetchMapCache += systemName -> systemFetchMap
  }

  /**
   * A helper method that updates MessageChooser. This should be called in
   * "choose" method after we try to consume a message from MessageChooser.
   */
  private def updateMessageChooser {
    buffer
      .flush
      // Let the fetchMap know of any SSPs that were given to the chooser, so 
      // a new fetch can be triggered if the buffer is low.
      .foreach(updateFetchMap(_))
  }
}
