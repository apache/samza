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

import _root_.kafka.consumer.SimpleConsumer
import _root_.kafka.utils.ZkUtils
import org.apache.kafka.common.utils.Utils
import org.apache.samza.checkpoint.{Checkpoint, CheckpointManager}
import org.apache.samza.container.TaskName
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.system._
import org.apache.samza.system.kafka.TopicMetadataCache
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
                              val getSystemConsumer: () => SystemConsumer,
                              val getSystemAdmin: () => SystemAdmin,
                              val metadataStore: TopicMetadataStore,
                              getSystemProducer: () => SystemProducer,
                              val connectZk: () => ZkUtils,
                              systemStreamPartitionGrouperFactoryString: String,
                              failOnCheckpointValidation: Boolean,
                              val retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy,
                              serde: CheckpointSerde = new CheckpointSerde,
                              checkpointTopicProperties: Properties = new Properties) extends CheckpointManager with Logging {
  import org.apache.samza.checkpoint.kafka.KafkaCheckpointManager._

  var taskNames = Set[TaskName]()
  //var producer: Producer[Array[Byte], Array[Byte]] = null
  var systemProducer : SystemProducer = null
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
        if (systemProducer == null) {
          systemProducer = getSystemProducer()
          systemProducer.register(taskName.getTaskName)
          systemProducer.start
        }

        //producer.send(new ProducerRecord(checkpointTopic, 0, keyBytes, msgBytes)).get()
        val systemStream = new SystemStream(systemName, checkpointTopic)
        val envelope : OutgoingMessageEnvelope = new OutgoingMessageEnvelope(systemStream, keyBytes, msgBytes)
        systemProducer.send(taskName.getTaskName, envelope)
        systemProducer.flush(taskName.getTaskName) // make sure it is written
        loop.done
      },

      (exception, loop) => {
        warn("Failed to write %s partition entry %s: %s. Retrying." format(CHECKPOINT_LOG4J_ENTRY, key, exception))
        debug("Exception detail:", exception)
        if (systemProducer != null) {
          systemProducer.stop
        }
        systemProducer = null
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


  private def getEarliestOffset(topic : String, partition: Partition): String = {
    val systemAdmin = getSystemAdmin()
    val metaData : java.util.Map[String, SystemStreamMetadata] = systemAdmin.getSystemStreamMetadata(Collections.singleton(topic))
    val checkpointMetadata:SystemStreamMetadata = metaData.get(topic)
    if(checkpointMetadata == null)
      throw new SamzaException("Cannot get metadata for system=%s, topic=%s" format (systemName, topic))

    val partitionMetaData = checkpointMetadata.getSystemStreamPartitionMetadata().get(partition);
    if(partitionMetaData == null)
      throw new SamzaException("Cannot get partitionMetaData for system=%s, topic=%s" format (systemName, topic))

    partitionMetaData.getOldestOffset
  }
  /**
   * Code for reading checkpoint log
   *
   * @param entryType What type of entry to look for within the log key's
   * @param handleEntry Code to handle an entry in the log once it's found
   */
  private def readLog(entryType:String, shouldHandleEntry: (KafkaCheckpointLogKey) => Boolean,
                      handleEntry: (ByteBuffer, KafkaCheckpointLogKey) => Unit): Unit = {

    val ssp: SystemStreamPartition = new SystemStreamPartition(systemName, checkpointTopic, new Partition(0))
    val systemConsumer = getSystemConsumer()
    val offset = getEarliestOffset(checkpointTopic, new Partition(0))
    systemConsumer.register(ssp, offset); // checkpoint stream should always be read from the beginning
    systemConsumer.start();

    var count = 0
    try {
      var done = false;
      while (!done) {
        // Map[SystemStreamPartition, List[IncomingMessageEnvelope]]
        val envelops = systemConsumer.poll(Collections.singleton(ssp), 1000)
        System.out.println("Checkpoint read %d envelops" format envelops.size());
        //val consumerRecords : ConsumerRecords[Long, String]  =
        val messages: util.List[IncomingMessageEnvelope] = envelops.get(ssp);
        if (envelops.isEmpty || messages.isEmpty) {
          // fully read the log
          done = true;
        }
        else {
          count += messages.size()
          // check the key
          for (msg: IncomingMessageEnvelope <- messages) {
            val key = msg.getKey.asInstanceOf[Array[Byte]]
            if (key == null)
              throw new KafkaUtilException("While reading checkpoint stream encountered message without key.")

            val checkpointKey = KafkaCheckpointLogKey.fromBytes(key)

            if (!shouldHandleEntry(checkpointKey)) {
              info("Skipping " + entryType + " entry with key " + checkpointKey)
            }
            else {
              val bbuf: java.nio.ByteBuffer = ByteBuffer.wrap(msg.getMessage.asInstanceOf[Array[Byte]])
              handleEntry(bbuf, checkpointKey)
            }
          }
        }
      }
    } finally {
      systemConsumer.stop();
    }
    System.out.println("DONE reading %s msgs from checkpoint stream %s:%s" format(count, systemName, checkpointTopic));

  }

  def start {
    kafkaUtil.createTopic(checkpointTopic, 1, replicationFactor, checkpointTopicProperties)
    kafkaUtil.validateTopicPartitionCount(checkpointTopic, systemName, metadataStore, 1, failOnCheckpointValidation)
  }

  def register(taskName: TaskName) {
    debug("Adding taskName " + taskName + " to " + this)
    taskNames += taskName
  }

  def stop = {
    if (systemProducer != null) {
      systemProducer.stop
      systemProducer = null
    }
  }

  override def toString = "KafkaCheckpointManager [systemName=%s, checkpointTopic=%s]" format(systemName, checkpointTopic)
}

object KafkaCheckpointManager {
  val CHECKPOINT_LOG4J_ENTRY = "checkpoint log"
  val CHANGELOG_PARTITION_MAPPING_LOG4j = "changelog partition mapping"
}
