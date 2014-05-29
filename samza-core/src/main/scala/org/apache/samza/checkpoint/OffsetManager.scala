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

package org.apache.samza.checkpoint

import org.apache.samza.system.SystemStream
import org.apache.samza.Partition
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.SystemStreamMetadata
import org.apache.samza.system.SystemStreamMetadata.OffsetType
import org.apache.samza.SamzaException
import scala.collection.JavaConversions._
import grizzled.slf4j.Logging
import org.apache.samza.config.Config
import org.apache.samza.config.StreamConfig.Config2Stream
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.system.SystemAdmin

/**
 * OffsetSetting encapsulates a SystemStream's metadata, default offset, and
 * reset offset settings. It's just a convenience class to make OffsetManager
 * easier to work with.
 */
case class OffsetSetting(
  /**
   * The metadata for the SystemStream.
   */
  metadata: SystemStreamMetadata,

  /**
   * The default offset (oldest, newest, or upcoming) for the SystemStream.
   * This setting is used when no checkpoint is available for a SystemStream
   * if the job is starting for the first time, or the SystemStream has been
   * reset (see resetOffsets, below).
   */
  defaultOffset: OffsetType,

  /**
   * Whether the SystemStream's offset should be reset or not. Determines
   * whether an offset should be ignored at initialization time, even if a
   * checkpoint is available. This is useful for jobs that wish to restart
   * reading from a stream at a different position than where they last
   * checkpointed. If this is true, then defaultOffset will be used to find
   * the new starting position in the stream.
   */
  resetOffset: Boolean)

/**
 * OffsetManager object is a helper that does wiring to build an OffsetManager
 * from a config object.
 */
object OffsetManager extends Logging {
  def apply(
    systemStreamMetadata: Map[SystemStream, SystemStreamMetadata],
    config: Config,
    checkpointManager: CheckpointManager = null,
    systemAdmins: Map[String, SystemAdmin] = Map()) = {

    debug("Building offset manager for %s." format systemStreamMetadata)

    val offsetSettings = systemStreamMetadata
      .map {
        case (systemStream, systemStreamMetadata) =>
          // Get default offset.
          val streamDefaultOffset = config.getDefaultStreamOffset(systemStream)
          val systemDefaultOffset = config.getDefaultSystemOffset(systemStream.getSystem)
          val defaultOffsetType = if (streamDefaultOffset.isDefined) {
            OffsetType.valueOf(streamDefaultOffset.get.toUpperCase)
          } else if (systemDefaultOffset.isDefined) {
            OffsetType.valueOf(systemDefaultOffset.get.toUpperCase)
          } else {
            info("No default offset for %s defined. Using upcoming." format systemStream)
            OffsetType.UPCOMING
          }
          debug("Using default offset %s for %s." format (defaultOffsetType, systemStream))

          // Get reset offset.
          val resetOffset = config.getResetOffset(systemStream)
          debug("Using reset offset %s for %s." format (resetOffset, systemStream))

          // Build OffsetSetting so we can create a map for OffsetManager.
          (systemStream, OffsetSetting(systemStreamMetadata, defaultOffsetType, resetOffset))
      }.toMap
    new OffsetManager(offsetSettings, checkpointManager, systemAdmins)
  }
}

/**
 * OffsetManager does several things:
 *
 * <ul>
 * <li>Loads last checkpointed offset for all input SystemStreamPartitions in a
 * SamzaContainer.</li>
 * <li>Uses last checkpointed offset to figure out the next offset to start
 * reading from for each input SystemStreamPartition in a SamzaContainer</li>
 * <li>Keep track of the last processed offset for each SystemStreamPartitions
 * in a SamzaContainer.</li>
 * <li>Checkpoints the last processed offset for each SystemStreamPartitions
 * in a SamzaContainer periodically to the CheckpointManager.</li>
 * </ul>
 *
 * All partitions must be registered before start is called, and start must be
 * called before get/update/checkpoint/stop are called.
 */
class OffsetManager(

  /**
   * Offset settings for all streams that the OffsetManager is managing.
   */
  val offsetSettings: Map[SystemStream, OffsetSetting] = Map(),

  /**
   * Optional checkpoint manager for checkpointing offsets whenever
   * checkpoint is called.
   */
  val checkpointManager: CheckpointManager = null,

  /**
   * SystemAdmins that are used to get next offsets from last checkpointed
   * offsets. Map is from system name to SystemAdmin class for the system.
   */
  val systemAdmins: Map[String, SystemAdmin] = Map()) extends Logging {

  /**
   * Last offsets processed for each SystemStreamPartition.
   */
  var lastProcessedOffsets = Map[SystemStreamPartition, String]()

  /**
   * Offsets to start reading from for each SystemStreamPartition. This
   * variable is populated after all checkpoints have been restored.
   */
  var startingOffsets = Map[SystemStreamPartition, String]()

  /**
   * The set of system stream partitions that have been registered with the
   * OffsetManager. These are the SSPs that will be tracked within the offset
   * manager.
   */
  var systemStreamPartitions = Set[SystemStreamPartition]()

  def register(systemStreamPartition: SystemStreamPartition) {
    systemStreamPartitions += systemStreamPartition
  }

  def start {
    registerCheckpointManager
    loadOffsetsFromCheckpointManager
    stripResetStreams
    loadStartingOffsets
    loadDefaults

    info("Successfully loaded last processed offsets: %s" format lastProcessedOffsets)
    info("Successfully loaded starting offsets: %s" format startingOffsets)
  }

  /**
   * Set the last processed offset for a given SystemStreamPartition.
   */
  def update(systemStreamPartition: SystemStreamPartition, offset: String) {
    lastProcessedOffsets += systemStreamPartition -> offset
  }

  /**
   * Get the last processed offset for a SystemStreamPartition.
   */
  def getLastProcessedOffset(systemStreamPartition: SystemStreamPartition) = {
    lastProcessedOffsets.get(systemStreamPartition)
  }

  /**
   * Get the starting offset for a SystemStreamPartition. This is the offset
   * where a SamzaContainer begins reading from when it starts up.
   */
  def getStartingOffset(systemStreamPartition: SystemStreamPartition) = {
    startingOffsets.get(systemStreamPartition)
  }

  /**
   * Checkpoint all offsets for a given partition using the CheckpointManager.
   */
  def checkpoint(partition: Partition) {
    if (checkpointManager != null) {
      debug("Checkpointing offsets for partition %s." format partition)

      val partitionOffsets = lastProcessedOffsets
        .filterKeys(_.getPartition.equals(partition))
        .map { case (systemStreamPartition, offset) => (systemStreamPartition.getSystemStream, offset) }
        .toMap

      checkpointManager.writeCheckpoint(partition, new Checkpoint(partitionOffsets))
    } else {
      debug("Skipping checkpointing for partition %s because no checkpoint manager is defined." format partition)
    }
  }

  def stop {
    if (checkpointManager != null) {
      debug("Shutting down checkpoint manager.")

      checkpointManager.stop
    } else {
      debug("Skipping checkpoint manager shutdown because no checkpoint manager is defined.")
    }
  }

  /**
   * Returns a set of partitions that have been registered with this offset
   * manager.
   */
  private def getPartitions = {
    systemStreamPartitions
      .map(_.getPartition)
      .toSet
  }

  /**
   * Register all partitions with the CheckpointManager.
   */
  private def registerCheckpointManager {
    if (checkpointManager != null) {
      debug("Registering checkpoint manager.")

      getPartitions.foreach(checkpointManager.register)
    } else {
      debug("Skipping checkpoint manager registration because no manager was defined.")
    }
  }

  /**
   * Loads last processed offsets from checkpoint manager for all registered
   * partitions.
   */
  private def loadOffsetsFromCheckpointManager {
    if (checkpointManager != null) {
      debug("Loading offsets from checkpoint manager.")

      checkpointManager.start

      lastProcessedOffsets ++= getPartitions
        .flatMap(restoreOffsetsFromCheckpoint(_))
        .filter {
          case (systemStreamPartition, offset) =>
            val shouldKeep = offsetSettings.contains(systemStreamPartition.getSystemStream)
            if (!shouldKeep) {
              info("Ignoring previously checkpointed offset %s for %s since the offset is for a stream that is not currently an input stream." format (offset, systemStreamPartition))
            }
            shouldKeep
        }
    } else {
      debug("Skipping offset load from checkpoint manager because no manager was defined.")
    }
  }

  /**
   * Loads last processed offsets for a single partition.
   */
  private def restoreOffsetsFromCheckpoint(partition: Partition): Map[SystemStreamPartition, String] = {
    debug("Loading checkpoints for partition: %s." format partition)

    val checkpoint = checkpointManager.readLastCheckpoint(partition)

    if (checkpoint != null) {
      checkpoint
        .getOffsets
        .map { case (systemStream, offset) => (new SystemStreamPartition(systemStream, partition), offset) }
        .toMap
    } else {
      info("Did not receive a checkpoint for partition %s. Proceeding without a checkpoint." format partition)

      Map()
    }
  }

  /**
   * Removes offset settings for all SystemStreams that are to be forcibly
   * reset using resetOffsets.
   */
  private def stripResetStreams {
    val systemStreamPartitionsToReset = getSystemStreamPartitionsToReset(lastProcessedOffsets.keys)

    systemStreamPartitionsToReset.foreach(systemStreamPartition => {
      val offset = lastProcessedOffsets(systemStreamPartition)
      info("Got offset %s for %s, but ignoring, since stream was configured to reset offsets." format (offset, systemStreamPartition))
    })

    lastProcessedOffsets --= systemStreamPartitionsToReset
  }

  /**
   * Returns a set of all SystemStreamPartitions in lastProcessedOffsets that need to be reset
   */
  private def getSystemStreamPartitionsToReset(systemStreamPartitions: Iterable[SystemStreamPartition]): Set[SystemStreamPartition] = {
    systemStreamPartitions
      .filter(systemStreamPartition => {
        val systemStream = systemStreamPartition.getSystemStream
        offsetSettings
          .getOrElse(systemStream, throw new SamzaException("Attempting to reset a stream that doesn't have offset settings %s." format systemStream))
          .resetOffset
      }).toSet
  }

  /**
   * Use last processed offsets to get next available offset for each
   * SystemStreamPartition, and populate startingOffsets.
   */
  private def loadStartingOffsets {
    startingOffsets ++= lastProcessedOffsets
      // Group offset map according to systemName.
      .groupBy(_._1.getSystem)
      // Get next offsets for each system.
      .flatMap {
        case (systemName, systemStreamPartitionOffsets) =>
          systemAdmins
            .getOrElse(systemName, throw new SamzaException("Missing system admin for %s. Need system admin to load starting offsets." format systemName))
            .getOffsetsAfter(systemStreamPartitionOffsets)
      }
  }

  /**
   * Use defaultOffsets to get a next offset for every SystemStreamPartition
   * that was registered, but has no offset.
   */
  private def loadDefaults {
    systemStreamPartitions.foreach(systemStreamPartition => {
      if (!startingOffsets.contains(systemStreamPartition)) {
        val systemStream = systemStreamPartition.getSystemStream
        val partition = systemStreamPartition.getPartition
        val offsetSetting = offsetSettings.getOrElse(systemStream, throw new SamzaException("Attempting to load defaults for stream %s, which has no offset settings." format systemStream))
        val systemStreamMetadata = offsetSetting.metadata
        val offsetType = offsetSetting.defaultOffset

        debug("Got default offset type %s for %s" format (offsetType, systemStreamPartition))

        val systemStreamPartitionMetadata = systemStreamMetadata
          .getSystemStreamPartitionMetadata
          .get(partition)

        if (systemStreamPartitionMetadata != null) {
          val nextOffset = systemStreamPartitionMetadata.getOffset(offsetType)

          debug("Got next default offset %s for %s" format (nextOffset, systemStreamPartition))

          startingOffsets += systemStreamPartition -> nextOffset
        } else {
          throw new SamzaException("No metadata available for partition %s." format systemStreamPartitionMetadata)
        }
      }
    })
  }
}
