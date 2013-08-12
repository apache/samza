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
import grizzled.slf4j.Logging
import org.apache.samza.serializers.SerdeManager

class SystemConsumers(
  picker: IncomingMessageEnvelopePicker,
  consumers: Map[String, SystemConsumer],
  serdeManager: SerdeManager,
  maxMsgsPerStreamPartition: Int = 1000,
  noNewMessagesTimeout: Long = 10) extends Logging {

  // TODO add metrics

  var unprocessedMessages = Map[SystemStreamPartition, Queue[IncomingMessageEnvelope]]()
  var neededByPicker = Set[SystemStreamPartition]()
  var fetchMap = Map[SystemStreamPartition, java.lang.Integer]()
  var timeout = noNewMessagesTimeout

  debug("Got stream consumers: %s" format consumers)
  debug("Got max messages per stream: %s" format maxMsgsPerStreamPartition)
  debug("Got no new message timeout: %s" format noNewMessagesTimeout)

  def start {
    debug("Starting consumers.")

    consumers.values.foreach(_.start)
  }

  def stop {
    debug("Stopping consumers.")

    consumers.values.foreach(_.stop)
  }

  def register(systemStreamPartition: SystemStreamPartition, lastReadOffset: String) {
    debug("Registering stream: %s, %s" format (systemStreamPartition, lastReadOffset))

    neededByPicker += systemStreamPartition
    fetchMap += systemStreamPartition -> maxMsgsPerStreamPartition
    unprocessedMessages += systemStreamPartition -> Queue[IncomingMessageEnvelope]()
    consumers(systemStreamPartition.getSystem).register(systemStreamPartition, lastReadOffset)
  }

  def pick = {
    val picked = picker.pick

    if (picked == null) {
      debug("Picker returned null.")

      // Allow blocking if the picker didn't pick a message.
      timeout = noNewMessagesTimeout
    } else {
      debug("Picker returned an incoming message envelope: %s" format picked)

      // Don't block if we have a message to process.
      timeout = 0

      // Ok to give the picker a new message from this stream.
      neededByPicker += picked.getSystemStreamPartition
    }

    refresh
    picked
  }

  private def refresh {
    debug("Refreshing picker with new messages.")

    // Poll every system for new messages.
    consumers.keys.foreach(poll(_))

    // Update the picker.
    neededByPicker.foreach(systemStreamPartition =>
      // If we have messages for a stream that the picker needs, then update.
      if (fetchMap(systemStreamPartition).intValue < maxMsgsPerStreamPartition) {
        picker.update(unprocessedMessages(systemStreamPartition).dequeue)
        fetchMap += systemStreamPartition -> (fetchMap(systemStreamPartition).intValue + 1)
        neededByPicker -= systemStreamPartition
      })
  }

  private def poll(systemName: String) = {
    debug("Polling system consumer: %s" format systemName)

    val consumer = consumers(systemName)

    debug("Filtering for system: %s, %s" format (systemName, fetchMap))

    val systemFetchMap = fetchMap.filterKeys(_.getSystem.equals(systemName))

    debug("Fetching: %s" format systemFetchMap)

    val incomingEnvelopes = consumer.poll(systemFetchMap, timeout)

    debug("Got incoming message envelopes: %s" format incomingEnvelopes)

    // We have new un-processed envelopes, so update maps accordingly.
    incomingEnvelopes.foreach(envelope => {
      val systemStreamPartition = envelope.getSystemStreamPartition

      debug("Got message for: %s, %s" format (systemStreamPartition, envelope))

      fetchMap += systemStreamPartition -> (fetchMap(systemStreamPartition).intValue - 1)

      debug("Updated fetch map for: %s, %s" format (systemStreamPartition, fetchMap))

      unprocessedMessages(envelope.getSystemStreamPartition).enqueue(serdeManager.fromBytes(envelope))

      debug("Updated unprocessed messages for: %s, %s" format (systemStreamPartition, unprocessedMessages))
    })
  }
}
