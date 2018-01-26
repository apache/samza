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



import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap

import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.SystemAdmins
import org.apache.samza.SamzaException
import org.apache.samza.config.Config
import org.apache.samza.config.StreamConfig.Config2Stream
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.container.TaskName
import org.apache.samza.system.SystemStreamMetadata.OffsetType
import org.apache.samza.system.{SystemAdmin, SystemStream, SystemStreamMetadata, SystemStreamPartition}
import org.apache.samza.util.Logging

import scala.collection.JavaConverters._
import scala.collection._

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
    systemAdmins: SystemAdmins = new SystemAdmins(new HashMap[String, SystemAdmin]),
    checkpointListeners: Map[String, CheckpointListener] = Map(),
    offsetManagerMetrics: OffsetManagerMetrics = new OffsetManagerMetrics) = {
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
    new OffsetManager(offsetSettings, checkpointManager, systemAdmins, checkpointListeners, offsetManagerMetrics)
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
  val systemAdmins: SystemAdmins = new SystemAdmins(new HashMap[String, SystemAdmin]),

  /**
   * Map of checkpointListeners for the systems that chose to provide one.
   * OffsetManager will call the listeners on each checkpoint.
   */
  checkpointListeners: Map[String, CheckpointListener] = Map(),

  /**
   * offsetManagerMetrics for keeping track of checkpointed offsets of each SystemStreamPartition.
   */
  val offsetManagerMetrics: OffsetManagerMetrics = new OffsetManagerMetrics) extends Logging {

  /**
   * Last offsets processed for each SystemStreamPartition.
   */
  val lastProcessedOffsets = new ConcurrentHashMap[TaskName, ConcurrentHashMap[SystemStreamPartition, String]]()

  /**
   * Offsets to start reading from for each SystemStreamPartition. This
   * variable is populated after all checkpoints have been restored.
   */
  var startingOffsets = Map[TaskName, Map[SystemStreamPartition, String]]()

  /**
   * The set of system stream partitions that have been registered with the
   * OffsetManager, grouped by the taskName they belong to. These are the SSPs
   * that will be tracked within the offset manager.
   */
  val systemStreamPartitions = mutable.Map[TaskName, mutable.Set[SystemStreamPartition]]()

  def register(taskName: TaskName, systemStreamPartitionsToRegister: Set[SystemStreamPartition]) {
    systemStreamPartitions.getOrElseUpdate(taskName, mutable.Set[SystemStreamPartition]()) ++= systemStreamPartitionsToRegister
    // register metrics
    systemStreamPartitions.foreach { case (taskName, ssp) => ssp.foreach (ssp => offsetManagerMetrics.addCheckpointedOffset(ssp, "")) }
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
  def update(taskName: TaskName, systemStreamPartition: SystemStreamPartition, offset: String) {
    lastProcessedOffsets.putIfAbsent(taskName, new ConcurrentHashMap[SystemStreamPartition, String]())
    if (offset != null && !offset.equals(IncomingMessageEnvelope.END_OF_STREAM_OFFSET)) {
      lastProcessedOffsets.get(taskName).put(systemStreamPartition, offset)
    }
  }

  /**
   * Get the last processed offset for a SystemStreamPartition.
   */
  def getLastProcessedOffset(taskName: TaskName, systemStreamPartition: SystemStreamPartition): Option[String] = {
    Option(lastProcessedOffsets.get(taskName)).map(_.get(systemStreamPartition))
  }

  /**
   * Get the starting offset for a SystemStreamPartition. This is the offset
   * where a SamzaContainer begins reading from when it starts up.
   */
  def getStartingOffset(taskName: TaskName, systemStreamPartition: SystemStreamPartition) = {
    startingOffsets.get(taskName) match {
      case Some(sspToOffsets) => sspToOffsets.get(systemStreamPartition)
      case None => None
    }
  }

  def setStartingOffset(taskName: TaskName, ssp: SystemStreamPartition, offset: String): Unit = {
    startingOffsets += taskName -> (startingOffsets(taskName) + (ssp -> offset))
  }

  /**
    * Gets a snapshot of all the current offsets for the specified task. This is useful to
    * ensure there are no concurrent updates to the offsets between when this method is
    * invoked and the corresponding call to [[OffsetManager.writeCheckpoint()]]
    */
  def buildCheckpoint(taskName: TaskName): Checkpoint = {
    if (checkpointManager != null || checkpointListeners.nonEmpty) {
      debug("Getting checkpoint offsets for taskName %s." format taskName)

      val sspsForTaskName = systemStreamPartitions.getOrElse(taskName, throw new SamzaException("No SSPs registered for task: " + taskName)).toSet
      val sspToOffsets = lastProcessedOffsets.get(taskName)

      val partitionOffsets = if (sspToOffsets != null) {
        // Filter the offsets in case the task model changed since the last checkpoint was written.
        sspToOffsets.asScala.filterKeys(sspsForTaskName.contains)
      } else {
        warn(taskName + " is not found... ")
        Map[SystemStreamPartition, String]()
      }

      new Checkpoint(new HashMap(partitionOffsets.asJava)) // Copy into new Map to prevent mutation
    } else {
      debug("Returning null checkpoint for taskName %s because no checkpoint manager/callback is defined." format taskName)
      null
    }
  }

  /**
    * Write the specified checkpoint for the given task.
    */
  def writeCheckpoint(taskName: TaskName, checkpoint: Checkpoint) {
    if (checkpoint != null && (checkpointManager != null || checkpointListeners.nonEmpty)) {
      debug("Writing checkpoint for taskName %s with offsets %s." format (taskName, checkpoint))

      if(checkpointManager != null) {
        checkpointManager.writeCheckpoint(taskName, checkpoint)
        val sspToOffsets = checkpoint.getOffsets
        if(sspToOffsets != null) {
          sspToOffsets.asScala.foreach {
            case (ssp, cp) => offsetManagerMetrics.checkpointedOffsets.get(ssp).set(cp)
          }
        }
      }

      // invoke checkpoint listeners
      checkpoint.getOffsets.asScala.groupBy { case (ssp, _) => ssp.getSystem }.foreach {
        case (systemName:String, offsets: Map[SystemStreamPartition, String]) => {
          // Option is empty if there is no checkpointListener for this systemName
          checkpointListeners.get(systemName).foreach(_.onCheckpoint(offsets.asJava))
        }
      }
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
   * Register all partitions with the CheckpointManager.
   */
  private def registerCheckpointManager {
    if (checkpointManager != null) {
      debug("Registering checkpoint manager.")
      systemStreamPartitions.keys.foreach(checkpointManager.register)
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
      val result = systemStreamPartitions
        .keys
        .flatMap(restoreOffsetsFromCheckpoint(_))
        .toMap
      result.map { case (taskName, sspToOffset) => {
          lastProcessedOffsets.put(taskName, new ConcurrentHashMap[SystemStreamPartition, String](sspToOffset.filter {
            case (systemStreamPartition, offset) =>
              val shouldKeep = offsetSettings.contains(systemStreamPartition.getSystemStream)
              if (!shouldKeep) {
                info("Ignoring previously checkpointed offset %s for %s since the offset is for a stream that is not currently an input stream." format (offset, systemStreamPartition))
              }
              info("Checkpointed offset is currently %s for %s" format (offset, systemStreamPartition))
              shouldKeep
          }.asJava))
        }
      }
    } else {
      debug("Skipping offset load from checkpoint manager because no manager was defined.")
    }
  }

  /**
   * Loads last processed offsets for a single taskName.
   */
  private def restoreOffsetsFromCheckpoint(taskName: TaskName): Map[TaskName, Map[SystemStreamPartition, String]] = {
    debug("Loading checkpoints for taskName: %s." format taskName)

    val checkpoint = checkpointManager.readLastCheckpoint(taskName)

    if (checkpoint != null) {
      Map(taskName -> checkpoint.getOffsets.asScala.toMap)
    } else {
      info("Did not receive a checkpoint for taskName %s. Proceeding without a checkpoint." format taskName)

      Map(taskName -> Map())
    }
  }

  /**
   * Removes offset settings for all SystemStreams that are to be forcibly
   * reset using resetOffsets.
   */
  private def stripResetStreams {
    val systemStreamPartitionsToReset = getSystemStreamPartitionsToReset(lastProcessedOffsets)

    systemStreamPartitionsToReset.foreach {
      case (taskName, systemStreamPartitions) => {
        systemStreamPartitions.foreach {
          systemStreamPartition =>
            {
              val offset = lastProcessedOffsets.get(taskName).get(systemStreamPartition)
              info("Got offset %s for %s, but ignoring, since stream was configured to reset offsets." format (offset, systemStreamPartition))
            }
        }
      }
    }

    lastProcessedOffsets.keys().asScala.foreach { taskName =>
      lastProcessedOffsets.get(taskName).keySet().removeAll(systemStreamPartitionsToReset(taskName).asJava)
    }
  }

  /**
   * Returns a map of all SystemStreamPartitions in lastProcessedOffsets that need to be reset
   */
  private def getSystemStreamPartitionsToReset(taskNameTosystemStreamPartitions: ConcurrentHashMap[TaskName, ConcurrentHashMap[SystemStreamPartition, String]]): Map[TaskName, Set[SystemStreamPartition]] = {
    taskNameTosystemStreamPartitions.asScala.map {
      case (taskName, sspToOffsets) => {
        taskName -> (sspToOffsets.asScala.filter {
          case (systemStreamPartition, offset) => {
            val systemStream = systemStreamPartition.getSystemStream
            offsetSettings
              .getOrElse(systemStream, throw new SamzaException("Attempting to reset a stream that doesn't have offset settings %s." format systemStream))
              .resetOffset
          }
        }.keys.toSet)
      }
    }
  }

  /**
   * Use last processed offsets to get next available offset for each
   * SystemStreamPartition, and populate startingOffsets.
   */
  private def loadStartingOffsets {
    startingOffsets = lastProcessedOffsets.asScala.map {
      case (taskName, sspToOffsets) => {
        taskName -> {
          sspToOffsets.asScala.groupBy(_._1.getSystem).flatMap {
            case (systemName, systemStreamPartitionOffsets) =>
              systemAdmins.getSystemAdmin(systemName).getOffsetsAfter(systemStreamPartitionOffsets.asJava).asScala
          }
        }
      }
    }
  }

  /**
   * Use defaultOffsets to get a next offset for every SystemStreamPartition
   * that was registered, but has no offset.
   */
  private def loadDefaults {
    val taskNameToSSPs: Map[TaskName, Set[SystemStreamPartition]] = systemStreamPartitions

    taskNameToSSPs.foreach {
      case (taskName, systemStreamPartitions) => {
        systemStreamPartitions.foreach { systemStreamPartition =>
          if (!startingOffsets.contains(taskName) || !startingOffsets(taskName).contains(systemStreamPartition)) {
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
              val nextOffset = {
                val requested = systemStreamPartitionMetadata.getOffset(offsetType)

                if (requested == null) {
                  warn("Requested offset type %s in %s, but the stream is empty. Defaulting to the upcoming offset." format (offsetType, systemStreamPartition))
                  systemStreamPartitionMetadata.getOffset(OffsetType.UPCOMING)
                } else requested
              }

              debug("Got next default offset %s for %s" format (nextOffset, systemStreamPartition))

              startingOffsets.get(taskName) match {
                case Some(sspToOffsets) => startingOffsets += taskName -> (sspToOffsets + (systemStreamPartition -> nextOffset))
                case None => startingOffsets += taskName -> Map(systemStreamPartition -> nextOffset)
              }

            } else {
              throw new SamzaException("No metadata available for partition %s." format systemStreamPartitionMetadata)
            }
          }
        }
      }
    }
  }
}
