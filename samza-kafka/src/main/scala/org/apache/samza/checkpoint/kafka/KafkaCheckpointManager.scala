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

package org.apache.samza.checkpoint.kafka

import java.nio.ByteBuffer
import java.util.{Collections, Properties}

import org.apache.kafka.common.utils.Utils
import org.apache.samza.checkpoint.{Checkpoint, CheckpointManager}
import org.apache.samza.config.{Config, JobConfig}
import org.apache.samza.container.TaskName
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.kafka.{KafkaCheckpointLogKey, KafkaCheckpointLogKeySerde, KafkaStreamSpec, KafkaSystemAdmin}
import org.apache.samza.system.{StreamSpec, SystemAdmin, _}
import org.apache.samza.util._
import org.apache.samza.{Partition, SamzaException}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Kafka checkpoint manager is used to store checkpoints in a Kafka topic.
  * To read a checkpoint for a specific taskName, we find the newest message
  * keyed to that taskName. If there is no such message, no checkpoint data
  * exists.  The underlying log has a single partition into which all
  * checkpoints and TaskName to changelog partition mappings are written.
  *
  * This class is thread safe for writing but not for reading checkpoints.
  * This is currently OK since checkpoints are only read on the main thread.
  */
class KafkaCheckpointManager(checkpointSpec: KafkaStreamSpec,
                             systemFactory: SystemFactory,
                             val metadataStore: TopicMetadataStore,
                             systemStreamPartitionGrouperFactoryString: String,
                             failOnCheckpointValidation: Boolean,
                             config: Config,
                             metricsRegistry: MetricsRegistry
                            ) extends CheckpointManager with Logging {

  val checkpointSystem = checkpointSpec.getSystemName
  val checkpointTopic = checkpointSpec.getPhysicalName
  val checkpointSsp: SystemStreamPartition = new SystemStreamPartition(checkpointSystem, checkpointTopic, new Partition(0))
  val checkpointMsgSerde: CheckpointSerde = new CheckpointSerde
  val checkpointKeySerde: KafkaCheckpointLogKeySerde = new KafkaCheckpointLogKeySerde

  @volatile var systemProducer: SystemProducer = null
  var systemConsumer: SystemConsumer = null
  val systemAdmin = systemFactory.getAdmin(checkpointSystem, config)

  var taskNames = Set[TaskName]()
  var taskNamesToOffsets: Map[TaskName, Checkpoint] = null
  val expectedGrouperFactoryClass = config.get(JobConfig.SSP_GROUPER_FACTORY)

  info("Creating KafkaCheckpointManager for checkpointTopic=%s, systemName=%s" format(checkpointTopic, checkpointSystem))

  /**
    * Write Checkpoint for specified taskName to log
    *
    * @param taskName   Specific Samza taskName of which to write a checkpoint of.
    * @param checkpoint Reference to a Checkpoint object to store offset data in.
    **/
  override def writeCheckpoint(taskName: TaskName, checkpoint: Checkpoint) {
    val key = new KafkaCheckpointLogKey(expectedGrouperFactoryClass, taskName, KafkaCheckpointLogKey.CHECKPOINT_TYPE)
    val keyBytes = checkpointKeySerde.toBytes(key)
    val msgBytes = checkpointMsgSerde.toBytes(checkpoint)
    val envelope = new OutgoingMessageEnvelope(checkpointSsp, keyBytes, msgBytes)
    val retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy

    retryBackoff.run(
      loop => {
        systemProducer.send(taskName.getTaskName, envelope)
        systemProducer.flush(taskName.getTaskName) // make sure it is written
        debug("Completed writing checkpoint=%s into %s topic for system %s." format(checkpoint, checkpointTopic, checkpointSystem))
        loop.done
      },

      (exception, loop) => {
        warn("Failed to write checkpoint log partition entry %s: %s. Retrying." format(key, exception))
        debug("Exception detail:", exception)
      }
    )
  }

  /**
    * Read the last checkpoint for specified TaskName
    *
    * @param taskName Specific Samza taskName for which to get the last checkpoint of.
    **/
  override def readLastCheckpoint(taskName: TaskName): Checkpoint = {
    if (!taskNames.contains(taskName)) {
      throw new SamzaException(taskName + " not registered with this CheckpointManager")
    }

    info("Reading checkpoint for taskName " + taskName)

    if (taskNamesToOffsets == null) {
      info("No TaskName to checkpoint mapping provided.  Reading for first time.")
      taskNamesToOffsets = readCheckpointsFromLog()
    } else {
      info("Already existing checkpoint mapping.  Merging new offsets")
      taskNamesToOffsets ++= readCheckpointsFromLog()
    }

    val checkpoint = taskNamesToOffsets.get(taskName).getOrElse(null)

    info("Got checkpoint state for taskName %s: %s" format(taskName, checkpoint))

    checkpoint
  }

  /**
    * Read through entire log, discarding changelog mapping, and building map of TaskNames to Checkpoints
    */
  def readCheckpointsFromLog(): Map[TaskName, Checkpoint] = {
    val checkpoints = mutable.Map[TaskName, Checkpoint]()

    def shouldHandleEntry(key: KafkaCheckpointLogKey) = true

    def handleCheckpoint(payload: ByteBuffer, checkpointKey: KafkaCheckpointLogKey): Unit = {
      val taskName = checkpointKey.getTaskName
      val checkpoint = checkpointMsgSerde.fromBytes(Utils.readBytes(payload))
      checkpoints.put(taskName, checkpoint) // replacing any existing, older checkpoints as we go
    }

    readLog(shouldHandleEntry, handleCheckpoint)
    checkpoints.toMap /* of the immutable kind */
  }

  private def getSSPMetadata(topic: String, partition: Partition): SystemStreamPartitionMetadata = {
    val metaDataMap: java.util.Map[String, SystemStreamMetadata] = systemAdmin.getSystemStreamMetadata(Collections.singleton(topic))
    val checkpointMetadata: SystemStreamMetadata = metaDataMap.get(topic)
    if (checkpointMetadata == null) {
      throw new SamzaException("Cannot get metadata for system=%s, topic=%s" format(checkpointSystem, topic))
    }

    val partitionMetaData = checkpointMetadata.getSystemStreamPartitionMetadata().get(partition)
    if (partitionMetaData == null) {
      throw new SamzaException("Cannot get partitionMetaData for system=%s, topic=%s" format(checkpointSystem, topic))
    }

    return partitionMetaData
  }

  /**
    * Reads an entry from the checkpoint log and invokes the provided lambda on it.
    *
    * @param handleEntry Code to handle an entry in the log once it's found
    */
  private def readLog(shouldHandleEntry: (KafkaCheckpointLogKey) => Boolean,
                      handleEntry: (ByteBuffer, KafkaCheckpointLogKey) => Unit): Unit = {
    info("Reading from checkpoint system:%s topic:%s" format(checkpointSystem, checkpointTopic))


    val iterator = new SystemStreamPartitionIterator(systemConsumer, checkpointSsp);
    var msgCount = 0
    while (iterator.hasNext) {
      val msg = iterator.next
      msgCount += 1

      val offset = msg.getOffset
      val key = msg.getKey.asInstanceOf[Array[Byte]]
      if (key == null) {
        throw new KafkaUtilException(
          "While reading checkpoint (currentOffset=%s) stream encountered message without key." format offset)
      }

      val checkpointKey = checkpointKeySerde.fromBytes(key)
      val grouperFactoryClass = checkpointKey.getGrouperFactoryClassName

      if (!expectedGrouperFactoryClass.equals(grouperFactoryClass)) {
        throw new SamzaException("SSPGrouperFactory in the checkpoint topic does not match the configured value" +
          s"Expected: $expectedGrouperFactoryClass Found: $grouperFactoryClass")
      }

      if (!shouldHandleEntry(checkpointKey)) {
        info("Skipping checkpoint log entry at offset %s with key %s." format(offset, checkpointKey))
      } else {
        val checkpointPayload = ByteBuffer.wrap(msg.getMessage.asInstanceOf[Array[Byte]])
        handleEntry(checkpointPayload, checkpointKey)
      }
    }
    info("Done reading %s messages from checkpoint system:%s topic:%s" format(msgCount, checkpointSystem, checkpointTopic))
  }

  override def start {
    val CHECKPOINT_STREAMID = "unused-temp-checkpoint-stream-id"

    if (systemProducer == null) {
      if (systemProducer == null) {
        systemProducer = systemFactory.getProducer(checkpointSystem, config, metricsRegistry)
        systemProducer.register("checkpoint-source")
        systemProducer.start
      }
    }

    if (systemConsumer == null) {
      val partitionMetadata = getSSPMetadata(checkpointTopic, new Partition(0))
      val oldestOffset = partitionMetadata.getOldestOffset

      systemConsumer = systemFactory.getConsumer(checkpointSystem, config, metricsRegistry)
      systemConsumer.register(checkpointSsp, oldestOffset)
      systemConsumer.start()
    }

    val spec = checkpointSpec

    info("About to create checkpoint stream: " + spec)
    systemAdmin.createStream(spec)
    info("Created checkpoint stream: " + spec)
    try {
      systemAdmin.validateStream(spec) // SPECIAL VALIDATION FOR CHECKPOINT. DO NOT FAIL IF failOnCheckpointValidation IS FALSE
      info("Validated spec: " + spec)
    } catch {
      case e: StreamValidationException =>
        if (failOnCheckpointValidation) {
          throw e
        } else {
          warn("Checkpoint stream validation partially failed. Ignoring it because failOnCheckpointValidation=" + failOnCheckpointValidation)
        }
      case e1: Exception => throw e1
    }
  }

  override def register(taskName: TaskName) {
    debug("Adding taskName " + taskName + " to " + this)
    taskNames += taskName
  }


  def stop = {
    if (systemProducer != null) {
      systemProducer.stop
      systemProducer = null
    }

    if (systemConsumer != null) {
      systemConsumer.stop
      systemConsumer = null
    }
  }

  override def clearCheckpoints = {
    info("Clear checkpoint stream %s in system %s" format(checkpointTopic, checkpointSystem))
    val spec = StreamSpec.createCheckpointStreamSpec(checkpointTopic, checkpointSystem)
    systemAdmin.clearStream(spec)
  }

  override def toString = "KafkaCheckpointManager [systemName=%s, checkpointTopic=%s]" format(checkpointSystem, checkpointTopic)
}
