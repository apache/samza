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
import java.util.Properties

import kafka.api._
import kafka.common.{ErrorMapping, InvalidMessageSizeException, TopicAndPartition, UnknownTopicOrPartitionException}
import kafka.consumer.SimpleConsumer
import kafka.message.InvalidMessageException
import kafka.utils.Utils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.samza.SamzaException
import org.apache.samza.checkpoint.{Checkpoint, CheckpointManager}
import org.apache.samza.container.TaskName
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.system.kafka.TopicMetadataCache
import org.apache.samza.util._

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
                              val metadataStore: TopicMetadataStore,
                              connectProducer: () => Producer[Array[Byte], Array[Byte]],
                              val connectZk: () => ZkClient,
                              systemStreamPartitionGrouperFactoryString: String,
                              val retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy,
                              serde: CheckpointSerde = new CheckpointSerde,
                              checkpointTopicProperties: Properties = new Properties) extends CheckpointManager with Logging {
  import org.apache.samza.checkpoint.kafka.KafkaCheckpointManager._

  var taskNames = Set[TaskName]()
  var producer: Producer[Array[Byte], Array[Byte]] = null
  var taskNamesToOffsets: Map[TaskName, Checkpoint] = null

  var startingOffset: Option[Long] = None // Where to start reading for each subsequent call of readCheckpoint
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
    retryBackoff.run(
      loop => {
        if (producer == null) {
          producer = connectProducer()
        }

        producer.send(new ProducerRecord(checkpointTopic, 0, keyBytes, msgBytes)).get()
        loop.done
      },

      (exception, loop) => {
        warn("Failed to write %s partition entry %s: %s. Retrying." format(CHECKPOINT_LOG4J_ENTRY, key, exception))
        debug("Exception detail:", exception)
        if (producer != null) {
          producer.close
        }
        producer = null
      }
    )
  }

  private def getConsumer(): SimpleConsumer = {
    val metadataMap = TopicMetadataCache.getTopicMetadata(Set(checkpointTopic), systemName, (topics: Set[String]) => metadataStore.getTopicInfo(topics))
    val metadata = metadataMap(checkpointTopic)
    val partitionMetadata = metadata.partitionsMetadata
      .filter(_.partitionId == 0)
      .headOption
      .getOrElse(throw new KafkaUtilException("Tried to find partition information for partition 0 for checkpoint topic, but it didn't exist in Kafka."))
    val leader = partitionMetadata
      .leader
      .getOrElse(throw new SamzaException("No leader available for topic %s" format checkpointTopic))

    info("Connecting to leader %s:%d for topic %s and to fetch all checkpoint messages." format(leader.host, leader.port, checkpointTopic))

    new SimpleConsumer(leader.host, leader.port, socketTimeout, bufferSize, clientId)
  }

  private def getEarliestOffset(consumer: SimpleConsumer, topicAndPartition: TopicAndPartition): Long = consumer.earliestOrLatestOffset(topicAndPartition, OffsetRequest.EarliestTime, -1)

  private def getOffset(consumer: SimpleConsumer, topicAndPartition: TopicAndPartition, earliestOrLatest: Long): Long = {
    val offsetResponse = consumer.getOffsetsBefore(new OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(earliestOrLatest, 1))))
      .partitionErrorAndOffsets
      .get(topicAndPartition)
      .getOrElse(throw new KafkaUtilException("Unable to find offset information for %s:0" format checkpointTopic))
    // Fail or retry if there was an an issue with the offset request.
    KafkaUtil.maybeThrowException(offsetResponse.error)

    val offset: Long = offsetResponse
      .offsets
      .headOption
      .getOrElse(throw new KafkaUtilException("Got response, but no offsets defined for %s:0" format checkpointTopic))

    offset
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
      debug("Adding checkpoint " + checkpoint + " for taskName " + taskName)
      checkpoints.put(taskName, checkpoint) // replacing any existing, older checkpoints as we go
    }

    readLog(CHECKPOINT_LOG4J_ENTRY, shouldHandleEntry, handleCheckpoint)
    checkpoints.toMap /* of the immutable kind */
  }


  /**
   * Common code for reading both changelog partition mapping and change log
   *
   * @param entryType What type of entry to look for within the log key's
   * @param handleEntry Code to handle an entry in the log once it's found
   */
  private def readLog(entryType:String, shouldHandleEntry: (KafkaCheckpointLogKey) => Boolean,
                      handleEntry: (ByteBuffer, KafkaCheckpointLogKey) => Unit): Unit = {
    retryBackoff.run[Unit](
      loop => {
        val consumer = getConsumer()

        val topicAndPartition = new TopicAndPartition(checkpointTopic, 0)

        try {
          var offset = startingOffset.getOrElse(getEarliestOffset(consumer, topicAndPartition))

          info("Got offset %s for topic %s and partition 0. Attempting to fetch messages for %s." format(offset, checkpointTopic, entryType))

          val latestOffset = getOffset(consumer, topicAndPartition, OffsetRequest.LatestTime)

          info("Get latest offset %s for topic %s and partition 0." format(latestOffset, checkpointTopic))

          if (offset < 0) {
            info("Got offset 0 (no messages in %s) for topic %s and partition 0, so returning empty collection. If you expected the checkpoint topic to have messages, you're probably going to lose data." format (entryType, checkpointTopic))
            return
          }

          while (offset < latestOffset) {
            val request = new FetchRequestBuilder()
              .addFetch(checkpointTopic, 0, offset, fetchSize)
              .maxWait(500)
              .minBytes(1)
              .clientId(clientId)
              .build

            val fetchResponse = consumer.fetch(request)
            if (fetchResponse.hasError) {
              warn("Got error code from broker for %s: %s" format(checkpointTopic, fetchResponse.errorCode(checkpointTopic, 0)))
              val errorCode = fetchResponse.errorCode(checkpointTopic, 0)
              if (ErrorMapping.OffsetOutOfRangeCode.equals(errorCode)) {
                warn("Got an offset out of range exception while getting last entry in %s for topic %s and partition 0, so returning a null offset to the KafkaConsumer. Let it decide what to do based on its autooffset.reset setting." format (entryType, checkpointTopic))
                return
              }
              KafkaUtil.maybeThrowException(errorCode)
            }

            for (response <- fetchResponse.messageSet(checkpointTopic, 0)) {
              offset = response.nextOffset
              startingOffset = Some(offset) // For next time we call

              if (!response.message.hasKey) {
                throw new KafkaUtilException("Encountered message without key.")
              }

              val checkpointKey = KafkaCheckpointLogKey.fromBytes(Utils.readBytes(response.message.key))

              if (!shouldHandleEntry(checkpointKey)) {
                debug("Skipping " + entryType + " entry with key " + checkpointKey)
              } else {
                handleEntry(response.message.payload, checkpointKey)
              }
            }
          }
        } finally {
          consumer.close()
        }

        loop.done
        Unit
      },

      (exception, loop) => {
        exception match {
          case e: InvalidMessageException => throw new KafkaUtilException("Got InvalidMessageException from Kafka, which is unrecoverable, so fail the samza job", e)
          case e: InvalidMessageSizeException => throw new KafkaUtilException("Got InvalidMessageSizeException from Kafka, which is unrecoverable, so fail the samza job", e)
          case e: UnknownTopicOrPartitionException => throw new KafkaUtilException("Got UnknownTopicOrPartitionException from Kafka, which is unrecoverable, so fail the samza job", e)
          case e: KafkaUtilException => throw e
          case e: Exception =>
            warn("While trying to read last %s entry for topic %s and partition 0: %s. Retrying." format(entryType, checkpointTopic, e))
            debug("Exception detail:", e)
        }
      }
    ).getOrElse(throw new SamzaException("Failed to get entries for " + entryType + " from topic " + checkpointTopic))

  }

  def start {
    kafkaUtil.createTopic(checkpointTopic, 1, replicationFactor, checkpointTopicProperties)
    kafkaUtil.validateTopicPartitionCount(checkpointTopic, systemName, metadataStore, 1)
  }

  def register(taskName: TaskName) {
    debug("Adding taskName " + taskName + " to " + this)
    taskNames += taskName
  }

  def stop = {
    if (producer != null) {
      producer.close
    }
  }


  /**
   * Read through entire log, discarding checkpoints, finding latest changelogPartitionMapping
   * To be used for Migration purpose only. In newer version, changelogPartitionMapping will be handled through coordinator stream
   */
  @Deprecated
  def readChangeLogPartitionMapping(): util.Map[TaskName, java.lang.Integer] = {
    var changelogPartitionMapping: util.Map[TaskName, java.lang.Integer] = new util.HashMap[TaskName, java.lang.Integer]()

    def shouldHandleEntry(key: KafkaCheckpointLogKey) = key.isChangelogPartitionMapping

    def handleCheckpoint(payload: ByteBuffer, checkpointKey:KafkaCheckpointLogKey): Unit = {
      changelogPartitionMapping = serde.changelogPartitionMappingFromBytes(Utils.readBytes(payload))

      debug("Adding changelog partition mapping" + changelogPartitionMapping)
    }

    readLog(CHANGELOG_PARTITION_MAPPING_LOG4j, shouldHandleEntry, handleCheckpoint)

    changelogPartitionMapping
  }

  override def toString = "KafkaCheckpointManager [systemName=%s, checkpointTopic=%s]" format(systemName, checkpointTopic)
}

object KafkaCheckpointManager {
  val CHECKPOINT_LOG4J_ENTRY = "checkpoint log"
  val CHANGELOG_PARTITION_MAPPING_LOG4j = "changelog partition mapping"
}
