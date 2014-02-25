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

import scala.collection.immutable.TreeMap
import org.apache.samza.SamzaException
import org.apache.samza.system.SystemStream
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.SystemStreamPartition
import grizzled.slf4j.Logging

/**
 * TieredPriorityChooser groups messages into priority tiers. Each priority
 * tier has its own MessageChooser. When an envelope arrives, its priority is
 * determined based on its SystemStream partition, and the MessageChooser for
 * the envelope's priority tier is updated.
 *
 * When choose is called, the TieredPriorityChooser calls choose on each
 * MessageChooser from the highest priority tier down to the lowest.
 * TieredPriorityChooser stops calling choose the first time that it gets a
 * non-null envelope from a MessageChooser.
 *
 * A higher number means a higher priority.
 *
 * For example, suppose that there are two SystemStreams, X and Y. X has a
 * priority of 1, and Y has a priority of 0. In this case, there are two tiers:
 * 1, and 0. Each tier has its own MessageChooser. When an envelope is received
 * via TieredPriorityChooser.update(), TieredPriorityChooser will determine the
 * priority of the stream (1 if X, 0 if Y), and update that tier's
 * MessageChooser. When MessageChooser.choose is called, TieredPriorityChooser
 * will first call choose on tier 1's MessageChooser. If this MessageChooser
 * returns null, then TieredPriorityChooser will call choose on tier 0's
 * MessageChooser. If neither return an envelope, then null is returned.
 *
 * This class is useful in cases where you wish to prioritize streams, and
 * always pick one over another. In such a case, you need a tie-breaker if
 * multiple envelopes exist that are of the same priority. This class uses
 * a tier's MessageChooser as the tie breaker when more than one envelope
 * exists with the same priority.
 */
class TieredPriorityChooser(
  /**
   * Map from stream to priority tier.
   */
  priorities: Map[SystemStream, Int],

  /**
   * Map from priority tier to chooser.
   */
  choosers: Map[Int, MessageChooser],

  /**
   * Default chooser to use if no priority is defined for an incoming
   * envelope's SystemStream. If null, an exception is thrown when an unknown
   * SystemStream is seen.
   */
  default: MessageChooser = null) extends MessageChooser with Logging {

  // Do a sanity check.
  priorities.values.toSet.foreach((priority: Int) =>
    if (!choosers.contains(priority)) {
      throw new SamzaException("Missing message chooser for priority: %s" format priority)
    })

  /**
   * A sorted list of MessageChoosers. Sorting is according to their priority,
   * from high to low.
   */
  val prioritizedChoosers = choosers
    .keys
    .toList
    .sortWith(_ > _)
    .map(choosers(_))

  /**
   * A map from a SystemStream to the MessageChooser that should be used for
   * the SystemStream.
   */
  val prioritizedStreams = priorities
    .map(systemStreamPriority => (systemStreamPriority._1, choosers.getOrElse(systemStreamPriority._2, throw new SamzaException("Unable to setup priority chooser. No chooser found for priority: %s" format systemStreamPriority._2))))
    .toMap

  def update(envelope: IncomingMessageEnvelope) {
    val systemStream = envelope.getSystemStreamPartition.getSystemStream
    val chooser = prioritizedStreams.get(systemStream) match {
      case Some(chooser) =>
        trace("Got prioritized chooser for stream: %s" format systemStream)

        chooser
      case _ =>
        trace("Trying default chooser because no priority is defined stream: %s" format systemStream)

        if (default != null) {
          default
        } else {
          throw new SamzaException("No default chooser defined, and no priority assigned to stream. Can't prioritize: %s" format envelope.getSystemStreamPartition)
        }
    }

    chooser.update(envelope)
  }

  /**
   * Choose a message from the highest priority MessageChooser. Keep going to
   * lower priority MessageChoosers until a non-null envelope is returned, or
   * the end of the list is reached. Return null if no envelopes are returned
   * from any MessageChoosers.
   */
  def choose = {
    var envelope: IncomingMessageEnvelope = null
    val iter = prioritizedChoosers.iterator

    while (iter.hasNext && envelope == null) {
      envelope = iter.next.choose
    }

    if (envelope == null && default != null) {
      trace("Got no prioritized envelope, so checking default chooser.")
      default.choose
    } else {
      trace("Got prioritized envelope: %s" format envelope)
      envelope
    }
  }

  def start = {
    info("Starting priority chooser with priorities: %s" format priorities)

    if (default != null) {
      info("Priority chooser has a default chooser: %s" format default)

      default.start
    }

    choosers.values.foreach(_.start)
  }

  def stop = {
    if (default != null) {
      default.stop
    }

    choosers.values.foreach(_.stop)
  }

  def register(systemStreamPartition: SystemStreamPartition, offset: String) = {
    if (default != null) {
      default.register(systemStreamPartition, offset)
    }

    choosers.values.foreach(_.register(systemStreamPartition, offset))
  }
}
