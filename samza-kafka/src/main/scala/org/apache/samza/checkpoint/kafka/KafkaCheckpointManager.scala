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
import java.util
import java.util.{Collections, Properties}

import kafka.utils.ZkUtils
import org.apache.kafka.common.utils.Utils
import org.apache.samza.checkpoint.{Checkpoint, CheckpointManager}
import org.apache.samza.container.TaskName
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.kafka.{KafkaSystemAdmin, KafkaStreamSpec}
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
 */
class KafkaCheckpointManager(
                              clientId: String,
                              checkpointTopic: String,
                              val systemName: String,
                              replicationFactor: Int,
                              socketTimeout: Int,
                              bufferSize: Int,
                              fetchSize: Int,
                              getSystemConsumer: () => SystemConsumer,
                              getSystemAdmin: () => SystemAdmin,
                              val metadataStore: TopicMetadataStore,
                              getSystemProducer: () => SystemProducer,
                              val connectZk: () => ZkUtils,
                              systemStreamPartitionGrouperFactoryString: String,
                              failOnCheckpointValidation: Boolean,
                              val retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy,
                              serde: CheckpointSerde = new CheckpointSerde,
                              checkpointTopicProperties: Properties = new Properties) extends CheckpointManager with Logging {

  var taskNames = Set[TaskName]()
  @volatile var  systemProducer: SystemProducer = null
  var taskNamesToOffsets: Map[TaskName, Checkpoint] = null
  val systemAdmin = getSystemAdmin()

  val kafkaUtil: KafkaUtil = new KafkaUtil(retryBackoff, connectZk)



  KafkaCheckpointLogKey.setSystemStreamPartitionGrouperFactoryString(systemStreamPartitionGrouperFactoryString)

  info("Creating KafkaCheckpointManager with: clientId=%s, checkpointTopic=%s, systemName=%s" format(clientId, checkpointTopic, systemName))

  /**
   * Write Checkpoint for specified taskName to log
   *
   * @param taskName Specific Samza taskName of which to write a checkpoint of.
   * @param checkpoint Reference to a Checkpoint object to store offset data in.
   **/
  override def writeCheckpoint(taskName: TaskName, checkpoint: Checkpoint) {
    val key = KafkaCheckpointLogKey.getCheckpointKey(taskName)
    val keyBytes = key.toBytes()
    val msgBytes = serde.toBytes(checkpoint)
    val systemStream = new SystemStream(systemName, checkpointTopic)
    val envelope = new OutgoingMessageEnvelope(systemStream, keyBytes, msgBytes)

    retryBackoff.run(
      loop => {
        if (systemProducer == null) {
          synchronized {
            if (systemProducer == null) {
              systemProducer = getSystemProducer()
              systemProducer.register(taskName.getTaskName)
              systemProducer.start
            }
          }
        }

        systemProducer.send(taskName.getTaskName, envelope)
        systemProducer.flush(taskName.getTaskName) // make sure it is written
        debug("Completed writing checkpoint=%s into %s topic for system %s." format(checkpoint, checkpointTopic, systemName) )
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

    def shouldHandleEntry(key: KafkaCheckpointLogKey) = key.isCheckpointKey

    def handleCheckpoint(payload: ByteBuffer, checkpointKey:KafkaCheckpointLogKey): Unit = {
      val taskName = checkpointKey.getCheckpointTaskName
      val checkpoint = serde.fromBytes(Utils.readBytes(payload))
      checkpoints.put(taskName, checkpoint) // replacing any existing, older checkpoints as we go
    }

    readLog(shouldHandleEntry, handleCheckpoint)
    checkpoints.toMap /* of the immutable kind */
  }

  private def getSSPMetadata(topic: String, partition: Partition): SystemStreamPartitionMetadata = {
    val metaDataMap: java.util.Map[String, SystemStreamMetadata] = systemAdmin.getSystemStreamMetadata(Collections.singleton(topic))
    val checkpointMetadata: SystemStreamMetadata = metaDataMap.get(topic)
    if (checkpointMetadata == null) {
      throw new SamzaException("Cannot get metadata for system=%s, topic=%s" format(systemName, topic))
    }

    val partitionMetaData = checkpointMetadata.getSystemStreamPartitionMetadata().get(partition)
    if (partitionMetaData == null) {
      throw new SamzaException("Cannot get partitionMetaData for system=%s, topic=%s" format(systemName, topic))
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

    val UNKNOWN_OFFSET = "-1"
    var attempts = 10
    val POLL_TIMEOUT = 1000L

    val ssp: SystemStreamPartition = new SystemStreamPartition(systemName, checkpointTopic, new Partition(0))
    val systemConsumer = getSystemConsumer()
    val partitionMetadata = getSSPMetadata(checkpointTopic, new Partition(0))
    // offsets returned are strings
    val newestOffset = if (partitionMetadata.getNewestOffset == null) UNKNOWN_OFFSET else partitionMetadata.getNewestOffset
    val oldestOffset = partitionMetadata.getOldestOffset
    systemConsumer.register(ssp, oldestOffset) // checkpoint stream should always be read from the beginning
    systemConsumer.start()

    var msgCount = 0
    try {
      // convert offsets to long
      var currentOffset = UNKNOWN_OFFSET.toLong
      val newestOffsetLong = newestOffset.toLong
      val sspToPoll = Collections.singleton(ssp)
      while (currentOffset < newestOffsetLong) {

        // will always read a single SSP
        val messages: java.util.List[IncomingMessageEnvelope] =
        try {
          systemConsumer.poll(sspToPoll, POLL_TIMEOUT).getOrDefault(ssp, Collections.emptyList())
        } catch {
          case e: Exception => {
            // these exceptions are most likely intermediate
            warn("Got %s exception while polling the consumer for checkpoints." format e)
            if (attempts == 0) throw new SamzaException("Multiple attempts failed while reading the checkpoints. Giving up.", e)
            attempts -= 1
            Collections.emptyList()
          }
        }

        debug("CheckpointMgr read %s messages from ssp %s. Current offset is %s, newest is %s"
                      format(messages.size(), ssp, currentOffset, newestOffset))
        if (messages.size() <= 0) {
          debug("Got empty/null list of messages")
        }
        else {
          msgCount += messages.size()
          // check the key
          for (msg: IncomingMessageEnvelope <- messages) {
            val key = msg.getKey.asInstanceOf[Array[Byte]]
            currentOffset = msg.getOffset().toLong
            if (key == null) {
              throw new KafkaUtilException(
                "While reading checkpoint (currentOffset=%s) stream encountered message without key."
                        format currentOffset)
            }

            val checkpointKey = KafkaCheckpointLogKey.fromBytes(key)

            if (!shouldHandleEntry(checkpointKey)) {
              info("Skipping checkpoint log entry at offset %s with key %s." format(currentOffset, checkpointKey))
            } else {
              // handleEntry requires ByteBuffer
              val checkpointPayload = ByteBuffer.wrap(msg.getMessage.asInstanceOf[Array[Byte]])
              handleEntry(checkpointPayload, checkpointKey)
            }
          }
        }
      }
    } finally {
      systemConsumer.stop()
    }
    info("Done reading %s messages from checkpoint system:%s topic:%s" format(msgCount, systemName, checkpointTopic))
  }

  override def start {
    val CHECKPOINT_STREAMID = "unused-temp-checkpoint-stream-id"
    val spec = new KafkaStreamSpec(CHECKPOINT_STREAMID,
                                   checkpointTopic, systemName, 1,
                                   replicationFactor, checkpointTopicProperties)

    info("About to create checkpoint stream: " + spec)
    systemAdmin.createStream(spec)
    info("Created checkpoint stream: " + spec)
    try {
      systemAdmin.validateStream(spec) // SPECIAL VALIDATION FOR CHECKPOINT. DO NOT FAIL IF failOnCheckpointValidation IS FALSE
      info("Validated spec: " + spec)
    } catch {
      case e : StreamValidationException =>
             if (failOnCheckpointValidation) {
               throw e
             } else {
               warn("Checkpoint stream validation partially failed. Ignoring it because failOnCheckpointValidation=" + failOnCheckpointValidation)
             }
      case e1 : Exception => throw e1
    }
  }

  override def register(taskName: TaskName) {
    debug("Adding taskName " + taskName + " to " + this)
    taskNames += taskName
  }


  def stop = {
    synchronized (
      if (systemProducer != null) {
        systemProducer.stop
        systemProducer = null
      }
    )

  }

  override def clearCheckpoints = {
    info("Clear checkpoint stream %s in system %s" format (checkpointTopic, systemName))
    val spec = StreamSpec.createCheckpointStreamSpec(checkpointTopic, systemName)
    systemAdmin.clearStream(spec)
  }

  override def toString = "KafkaCheckpointManager [systemName=%s, checkpointTopic=%s]" format(systemName, checkpointTopic)
}
