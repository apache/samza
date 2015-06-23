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

import java.util.concurrent.atomic.AtomicInteger
import org.apache.samza.system.SystemStream
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.util.Logging
import org.apache.samza.metrics.MetricsHelper
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.SystemStreamMetadata
import scala.collection.JavaConversions._
import org.apache.samza.SamzaException
import org.apache.samza.system.SystemStreamMetadata.OffsetType

/**
 * BootstrappingChooser is a composable MessageChooser that only chooses
 * an envelope when it's received at least one envelope for each SystemStream.
 * It does this by only allowing wrapped.choose to be called when the wrapped
 * MessageChooser has been updated with at least one envelope for every
 * SystemStream defined in the latestMessageOffsets map. Thus, the guarantee
 * is that the wrapped chooser will have an envelope from each SystemStream
 * whenever it has to make a choice about which envelope to process next.
 *
 * This behavior continues for each SystemStream that has lagging partitions.
 * As a SystemStream catches up to head, it is no longer marked as lagging,
 * and the requirement that the wrapped chooser have an envelope from the
 * SystemStream is dropped. Once all SystemStreams have caught up, this
 * MessageChooser just becomes a pass-through that always delegates to the
 * wrapped chooser.
 *
 * If a SystemStream falls behind after the initial catch-up, this chooser
 * makes no effort to catch the SystemStream back up, again.
 */
class BootstrappingChooser(
  /**
   * The message chooser that BootstrappingChooser delegates to when it's
   * updating or choosing envelopes.
   */
  wrapped: MessageChooser,

  /**
   * A map from system stream to metadata information, which includes oldest,
   * newest, and upcoming offsets for each partition. If a stream does not need to
   * be guaranteed available to the underlying wrapped chooser, it should not
   * be included in this map.
   */
  var bootstrapStreamMetadata: Map[SystemStream, SystemStreamMetadata] = Map(),

  /**
   * An object that holds all of the metrics related to bootstrapping.
   */
  metrics: BootstrappingChooserMetrics = new BootstrappingChooserMetrics) extends MessageChooser with Logging {

  /**
   * The number of lagging partitions for each SystemStream that's behind.
   */
  var systemStreamLagCounts = bootstrapStreamMetadata
    .mapValues(_.getSystemStreamPartitionMetadata.size)

  /**
   * All SystemStreamPartitions that are lagging.
   */
  var laggingSystemStreamPartitions = bootstrapStreamMetadata
    .flatMap {
      case (systemStream, metadata) =>
        metadata
          .getSystemStreamPartitionMetadata
          .keys
          .map(new SystemStreamPartition(systemStream, _))
    }
    .toSet

  /**
   * Store all the systemStreamPartitions registered
   */
  var registeredSystemStreamPartitions = Set[SystemStreamPartition]()

  /**
   * The number of lagging partitions that the underlying wrapped chooser has
   * been updated with, grouped by SystemStream.
   */
  var updatedSystemStreams = Map[SystemStream, Int]()

  def start = {
    // remove the systemStreamPartitions not registered.
    laggingSystemStreamPartitions = laggingSystemStreamPartitions.filter(registeredSystemStreamPartitions.contains(_))
    systemStreamLagCounts = laggingSystemStreamPartitions.groupBy(_.getSystemStream).map {case (systemStream, ssps) => systemStream -> ssps.size}

    debug("Starting bootstrapping chooser with bootstrap metadata: %s" format bootstrapStreamMetadata)
    info("Got lagging partition counts for bootstrap streams: %s" format systemStreamLagCounts)
    metrics.setLaggingSystemStreams(() => laggingSystemStreamPartitions.size)
    systemStreamLagCounts.keys.foreach { (systemStream: SystemStream) =>
      metrics.setLagCount(systemStream, () => systemStreamLagCounts.getOrElse(systemStream, 0))
    }
    wrapped.start
  }

  def stop = wrapped.stop

  override def register(systemStreamPartition: SystemStreamPartition, offset: String) {
    debug("Registering stream partition with offset: %s, %s" format (systemStreamPartition, offset))

    // If the offset we're starting to consume from is the same as the upcoming 
    // offset for this system stream partition, then we've already read all
    // messages in the stream, and we're at head for this system stream 
    // partition.
    checkOffset(systemStreamPartition, offset, OffsetType.UPCOMING)

    wrapped.register(systemStreamPartition, offset)

    registeredSystemStreamPartitions += systemStreamPartition
  }

  def update(envelope: IncomingMessageEnvelope) {
    wrapped.update(envelope)

    // If this is an SSP that is still lagging, update the count for the stream.
    if (laggingSystemStreamPartitions.contains(envelope.getSystemStreamPartition)) {
      trace("Bumping available message count for stream partition: %s" format envelope.getSystemStreamPartition)

      val systemStream = envelope.getSystemStreamPartition.getSystemStream

      updatedSystemStreams += systemStream -> (updatedSystemStreams.getOrElse(systemStream, 0) + 1)
    }
  }

  /**
   * If choose is called, and the parent MessageChoser has received an
   * envelope from at least one partition in each lagging SystemStream, then
   * the choose call is forwarded  to the wrapped chooser. Otherwise, the
   * BootstrappingChooser simply returns null, and waits for more updates.
   */
  def choose = {
    // If no system streams are behind, then go straight to the wrapped chooser.
    if (laggingSystemStreamPartitions.size == 0) {
      trace("No streams are lagging, so bypassing bootstrap chooser.")

      wrapped.choose
    } else if (okToChoose) {
      trace("Choosing from wrapped chooser, since wrapped choser has an envelope from all bootstrap streams.")

      val envelope = wrapped.choose

      if (envelope != null) {
        trace("Wrapped chooser chose non-null envelope: %s" format envelope)

        val systemStreamPartition = envelope.getSystemStreamPartition
        val offset = envelope.getOffset

        // Chosen envelope was from a bootstrap SSP, so decrement the update map.
        if (laggingSystemStreamPartitions.contains(systemStreamPartition)) {
          val systemStream = systemStreamPartition.getSystemStream

          updatedSystemStreams += systemStream -> (updatedSystemStreams.getOrElse(systemStream, 0) - 1)
        }

        // If the offset we just read is the same as the offset for the last 
        // message (newest) in this system stream partition, then we have read 
        // all messages, and can mark this SSP as bootstrapped.
        checkOffset(systemStreamPartition, offset, OffsetType.NEWEST)
      }

      envelope
    } else {
      trace("Blocking wrapped.chooser since bootstrapping is not done, but not all streams have messages available.")
      null
    }
  }

  /**
   * Checks to see if a bootstrap stream is fully caught up. If it is, the
   * state of the bootstrap chooser is updated to remove the system stream
   * from the set of lagging system streams.
   *
   * A SystemStreamPartition can be deemed "caught up" in one of two ways.
   * First, if a SystemStreamPartition is registered with a starting offset
   * that's equal to the upcoming offset for the SystemStreamPartition, then
   * it's "caught up". For example, if a SystemStreamPartition were registered
   * to start reading from offset 7, and the upcoming offset for the
   * SystemStreamPartition is also 7, then all prior messages are assumed to
   * already have been chosen, and the stream is marked as bootstrapped.
   * Second, if the offset for a chosen message equals the newest offset for the
   * message's SystemStreamPartition, then that SystemStreamPartition is deemed
   * caught up, because all messages in the stream up to the "newest" message
   * have been chosen.
   *
   * Note that the definition of "caught up" here is defined to be when all
   * messages that existed at container start time have been processed. If a
   * SystemStreamPartition's newest message offset is 8 at the time that a
   * container starts, but two more messages are written to the
   * SystemStreamPartition while the container is bootstrapping, the
   * SystemStreamPartition is marked as bootstrapped when the message with
   * offset 8 is chosen, not when the message with offset 10 is chosen.
   *
   * @param systemStreamPartition The SystemStreamPartition to check.
   * @param offset The offset of the most recently chosen message.
   * @param offsetType Whether to check the offset against the newest or
   *                   upcoming offset for the SystemStreamPartition.
   *                   Upcoming is useful during the registration phase,
   *                   and newest is useful during the choosing phase.
   */
  private def checkOffset(systemStreamPartition: SystemStreamPartition, offset: String, offsetType: OffsetType) {
    val systemStream = systemStreamPartition.getSystemStream
    val systemStreamMetadata = bootstrapStreamMetadata.getOrElse(systemStreamPartition.getSystemStream, null)
    // Metadata for system/stream, and system/stream/partition are allowed to 
    // be null since not all streams are bootstrap streams.
    val systemStreamPartitionMetadata = if (systemStreamMetadata != null) {
      systemStreamMetadata
        .getSystemStreamPartitionMetadata
        .get(systemStreamPartition.getPartition)
    } else {
      null
    }
    val offsetToCheck = if (systemStreamPartitionMetadata == null) {
      // Use null for offsetToCheck in cases where the partition metadata was 
      // null. A null partition metadata implies that the stream is not a 
      // bootstrap stream, and therefore, there is no need to check its offset.
      null
    } else {
      systemStreamPartitionMetadata.getOffset(offsetType)
    }

    trace("Check %s offset %s against %s for %s." format (offsetType, offset, offsetToCheck, systemStreamPartition))

    // The SSP is no longer lagging if the envelope's offset equals the 
    // latest offset. 
    if (offset != null && offset.equals(offsetToCheck)) {
      laggingSystemStreamPartitions -= systemStreamPartition
      systemStreamLagCounts += systemStream -> (systemStreamLagCounts(systemStream) - 1)

      debug("Bootstrap stream partition is fully caught up: %s" format systemStreamPartition)

      if (systemStreamLagCounts(systemStream) == 0) {
        info("Bootstrap stream is fully caught up: %s" format systemStream)

        // If the lag count is 0, then no partition for this stream is lagging 
        // (the stream has been fully caught up).
        systemStreamLagCounts -= systemStream
      }
    }
  }

  /**
   * It's only OK to allow the wrapped MessageChooser to choose if it's been
   * given at least one envelope from each lagging SystemStream.
   */
  private def okToChoose = {
    updatedSystemStreams.values.filter(_ > 0).size == laggingSystemStreamPartitions.groupBy(_.getSystemStream).size
  }
}

class BootstrappingChooserMetrics(val registry: MetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {
  val batches = newCounter("batch-resets")

  def setLaggingSystemStreams(getValue: () => Int) {
    newGauge("lagging-batch-streams", getValue)
  }

  def setLagCount(systemStream: SystemStream, getValue: () => Int) {
    newGauge("%s-%s-lagging-partitions" format (systemStream.getSystem, systemStream.getStream), getValue)
  }
}
