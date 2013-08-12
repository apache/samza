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

import org.I0Itec.zkclient.ZkClient

import grizzled.slf4j.Logging
import kafka.admin.AdminUtils
import kafka.api.FetchRequestBuilder
import kafka.api.OffsetRequest
import kafka.api.PartitionOffsetRequestInfo
import kafka.common.ErrorMapping
import kafka.common.TopicAndPartition
import kafka.common.TopicExistsException
import kafka.consumer.SimpleConsumer
import kafka.producer.KeyedMessage
import kafka.producer.Partitioner
import kafka.producer.Producer
import kafka.serializer.Decoder
import kafka.serializer.Encoder
import kafka.utils.Utils
import kafka.utils.VerifiableProperties
import org.apache.samza.Partition
import org.apache.samza.SamzaException
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.checkpoint.CheckpointManager
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.serializers.Serde
import org.apache.samza.system.kafka.TopicMetadataCache
import org.apache.samza.util.TopicMetadataStore

/**
 * Kafka checkpoint manager is used to store checkpoints in a Kafka topic that
 * is uniquely identified by a job/partition combination. To read a checkpoint
 * for a given job and partition combination (e.g. my-job, partition 1), we
 * simply read the last message from the topic: __samza_checkpoint_my-job_1. If
 * the topic does not yet exist, we assume that there is not yet any state for
 * this job/partition pair, and return an empty checkpoint.
 */
class KafkaCheckpointManager(
  clientId: String,
  stateTopic: String,
  systemName: String,
  totalPartitions: Int,
  replicationFactor: Int,
  socketTimeout: Int,
  bufferSize: Int,
  fetchSize: Int,
  metadataStore: TopicMetadataStore,
  connectProducer: () => Producer[Partition, Array[Byte]],
  connectZk: () => ZkClient,
  failureRetryMs: Long = 10000,
  serde: Serde[Checkpoint] = new CheckpointSerde) extends CheckpointManager with Logging {

  var partitions = Set[Partition]()
  var producer: Producer[Partition, Array[Byte]] = null

  info("Creating KafkaCheckpointManager with: clientId=%s, stateTopic=%s, systemName=%s" format (clientId, stateTopic, systemName))

  def writeCheckpoint(partition: Partition, checkpoint: Checkpoint) {
    var done = false

    while (!done) {
      try {
        if (producer == null) {
          producer = connectProducer()
        }

        producer.send(new KeyedMessage(stateTopic, null, partition, serde.toBytes(checkpoint)))
        done = true
      } catch {
        case e: Throwable =>
          warn("Failed to send checkpoint %s for partition %s. Retrying." format (checkpoint, partition), e)

          if (producer != null) {
            producer.close
          }

          producer = null

          Thread.sleep(failureRetryMs)
      }
    }
  }

  def readLastCheckpoint(partition: Partition): Checkpoint = {
    var checkpoint: Option[Checkpoint] = None
    var consumer: SimpleConsumer = null

    info("Reading checkpoint for partition %s." format partition.getPartitionId)

    while (!checkpoint.isDefined) {
      try {
        // Assume state topic exists with correct partitions, since it should be verified on start.
        // Fetch the metadata for this state topic/partition pair.
        val metadataMap = TopicMetadataCache.getTopicMetadata(Set(stateTopic), systemName, (topics: Set[String]) => metadataStore.getTopicInfo(topics))
        val metadata = metadataMap(stateTopic)
        val partitionMetadata = metadata.partitionsMetadata
          .filter(_.partitionId == partition.getPartitionId)
          .headOption
          .getOrElse(throw new KafkaCheckpointException("Tried to find partition information for partition %d, but it didn't exist in Kafka." format partition.getPartitionId))
        val partitionId = partitionMetadata.partitionId
        val leader = partitionMetadata
          .leader
          .getOrElse(throw new SamzaException("No leader available for topic %s" format stateTopic))

        info("Connecting to leader %s:%d for topic %s and partition %s to fetch last checkpoint message." format (leader.host, leader.port, stateTopic, partitionId))

        consumer = new SimpleConsumer(
          leader.host,
          leader.port,
          socketTimeout,
          bufferSize,
          clientId)
        val topicAndPartition = new TopicAndPartition(stateTopic, partitionId)
        val offset = consumer.getOffsetsBefore(new OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1))))
          .partitionErrorAndOffsets
          .get(topicAndPartition)
          .getOrElse(throw new KafkaCheckpointException("Unable to find offset information for %s:%d" format (stateTopic, partitionId)))
          .offsets
          .headOption
          .getOrElse(throw new KafkaCheckpointException("Got response, but no offsets defined for %s:%d" format (stateTopic, partitionId)))

        info("Got offset %s for topic %s and partition %s. Attempting to fetch message." format (offset, stateTopic, partitionId))

        if (offset <= 0) {
          info("Got offset 0 (no messages in state topic) for topic %s and partition %s, so returning null. If you expected the state topic to have messages, you're probably going to lose data." format (stateTopic, partition))
          return null
        }

        val request = new FetchRequestBuilder()
          // Kafka returns 1 greater than the offset of the last message in 
          //the topic, so subtract one to fetch the last message.
          .addFetch(stateTopic, partitionId, offset - 1, fetchSize)
          .maxWait(500)
          .minBytes(1)
          .clientId(clientId)
          .build
        val messageSet = consumer.fetch(request)
        if (messageSet.hasError) {
          warn("Got error code from broker for %s: %s" format (stateTopic, messageSet.errorCode(stateTopic, partitionId)))
          val errorCode = messageSet.errorCode(stateTopic, partitionId)
          if (ErrorMapping.OffsetOutOfRangeCode.equals(errorCode)) {
            warn("Got an offset out of range exception while getting last checkpoint for topic %s and partition %s, so returning a null offset to the KafkaConsumer. Let it decide what to do based on its autooffset.reset setting." format (stateTopic, partitionId))
            return null
          }
          ErrorMapping.maybeThrowException(errorCode)
        }
        val messages = messageSet.messageSet(stateTopic, partitionId).toList

        if (messages.length != 1) {
          throw new KafkaCheckpointException("Something really unexpected happened. Got %s "
            + "messages back when fetching from state checkpoint topic %s and partition %s. "
            + "Expected one message. It would be unsafe to go on without the latest checkpoint, "
            + "so failing." format (messages.length, stateTopic, partition))
        }

        // Some back bending to go from message to checkpoint.
        checkpoint = Some(serde.fromBytes(Utils.readBytes(messages(0).message.payload)))

        consumer.close
      } catch {
        case e: KafkaCheckpointException =>
          throw e
        case e: Throwable =>
          warn("Got exception while trying to read last checkpoint for topic %s and partition %s. Retrying." format (stateTopic, partition), e)

          if (consumer != null) {
            consumer.close
          }

          Thread.sleep(failureRetryMs)
      }
    }

    info("Got checkpoint state for partition %s: %s" format (partition.getPartitionId, checkpoint))

    checkpoint.get
  }

  def start {
    if (partitions.contains(new Partition(0))) {
      createTopic
    }

    validateTopic
  }

  def register(partition: Partition) {
    partitions += partition
  }

  def stop = producer.close

  private def createTopic {
    var done = false
    var zkClient: ZkClient = null

    info("Attempting to create state topic %s with %s partitions." format (stateTopic, totalPartitions))

    while (!done) {
      try {
        zkClient = connectZk()

        AdminUtils.createTopic(
          zkClient,
          stateTopic,
          totalPartitions,
          replicationFactor)

        info("Created state topic %s." format stateTopic)

        done = true
      } catch {
        case e: TopicExistsException =>
          info("State topic %s already exists." format stateTopic)

          done = true
        case e: Throwable =>
          warn("Failed to create topic %s. Retrying." format stateTopic, e)

          if (zkClient != null) {
            zkClient.close
          }

          Thread.sleep(failureRetryMs)
      }
    }

    zkClient.close
  }

  private def validateTopic {
    var done = false

    info("Validating state topic %s." format stateTopic)

    while (!done) {
      try {
        val topicMetadataMap = TopicMetadataCache.getTopicMetadata(Set(stateTopic), systemName, metadataStore.getTopicInfo)
        val topicMetadata = topicMetadataMap(stateTopic)
        val errorCode = topicMetadata.errorCode

        if (errorCode != ErrorMapping.NoError) {
          throw new SamzaException("State topic validation failed for topic %s because we got error code %s from Kafka." format (stateTopic, errorCode))
        }

        val partitionCount = topicMetadata.partitionsMetadata.length

        if (partitionCount != totalPartitions) {
          throw new KafkaCheckpointException("State topic validation failed for topic %s because partition count %s did not match expected partition count %s." format (stateTopic, topicMetadata.partitionsMetadata.length, totalPartitions))
        }

        info("Successfully validated state topic %s." format stateTopic)

        done = true
      } catch {
        case e: KafkaCheckpointException =>
          throw e
        case e: Throwable =>
          warn("Got exception while trying to read validate topic %s. Retrying." format stateTopic, e)

          Thread.sleep(failureRetryMs)
      }
    }
  }
}

/**
 * KafkaCheckpointManager handles retries, so we need two kinds of exceptions:
 * one to signal a hard failure, and the other to retry. The
 * KafkaCheckpointException is thrown to indicate a hard failure that the Kafka
 * CheckpointManager can't recover from.
 */
class KafkaCheckpointException(s: String, t: Throwable) extends SamzaException(s, t) {
  def this(s: String) = this(s, null)
}
