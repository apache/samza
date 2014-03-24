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
import org.apache.samza.util.ExponentialSleepStrategy

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
  checkpointTopic: String,
  systemName: String,
  totalPartitions: Int,
  replicationFactor: Int,
  socketTimeout: Int,
  bufferSize: Int,
  fetchSize: Int,
  metadataStore: TopicMetadataStore,
  connectProducer: () => Producer[Partition, Array[Byte]],
  connectZk: () => ZkClient,
  retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy,
  serde: Serde[Checkpoint] = new CheckpointSerde) extends CheckpointManager with Logging {

  var partitions = Set[Partition]()
  var producer: Producer[Partition, Array[Byte]] = null

  info("Creating KafkaCheckpointManager with: clientId=%s, checkpointTopic=%s, systemName=%s" format (clientId, checkpointTopic, systemName))

  def writeCheckpoint(partition: Partition, checkpoint: Checkpoint) {
    retryBackoff.run(
      loop => {
        if (producer == null) {
          producer = connectProducer()
        }
        producer.send(new KeyedMessage(checkpointTopic, null, partition, serde.toBytes(checkpoint)))
        loop.done
      },

      (exception, loop) => {
        warn("Failed to send checkpoint %s for partition %s: %s. Retrying." format (checkpoint, partition, exception))
        debug(exception)
        if (producer != null) {
          producer.close
        }
        producer = null
      }
    )
  }

  def readLastCheckpoint(partition: Partition): Checkpoint = {
    info("Reading checkpoint for partition %s." format partition.getPartitionId)

    val checkpoint = retryBackoff.run(
      loop => {
        // Assume checkpoint topic exists with correct partitions, since it should be verified on start.
        // Fetch the metadata for this checkpoint topic/partition pair.
        val metadataMap = TopicMetadataCache.getTopicMetadata(Set(checkpointTopic), systemName, (topics: Set[String]) => metadataStore.getTopicInfo(topics))
        val metadata = metadataMap(checkpointTopic)
        val partitionMetadata = metadata.partitionsMetadata
          .filter(_.partitionId == partition.getPartitionId)
          .headOption
          .getOrElse(throw new KafkaCheckpointException("Tried to find partition information for partition %d, but it didn't exist in Kafka." format partition.getPartitionId))
        val partitionId = partitionMetadata.partitionId
        val leader = partitionMetadata
          .leader
          .getOrElse(throw new SamzaException("No leader available for topic %s" format checkpointTopic))

        info("Connecting to leader %s:%d for topic %s and partition %s to fetch last checkpoint message." format (leader.host, leader.port, checkpointTopic, partitionId))

        val consumer = new SimpleConsumer(
          leader.host,
          leader.port,
          socketTimeout,
          bufferSize,
          clientId)
        try {
          val topicAndPartition = new TopicAndPartition(checkpointTopic, partitionId)
          val offsetResponse = consumer.getOffsetsBefore(new OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1))))
            .partitionErrorAndOffsets
            .get(topicAndPartition)
            .getOrElse(throw new KafkaCheckpointException("Unable to find offset information for %s:%d" format (checkpointTopic, partitionId)))

          // Fail or retry if there was an an issue with the offset request.
          ErrorMapping.maybeThrowException(offsetResponse.error)

          val offset = offsetResponse
            .offsets
            .headOption
            .getOrElse(throw new KafkaCheckpointException("Got response, but no offsets defined for %s:%d" format (checkpointTopic, partitionId)))

          info("Got offset %s for topic %s and partition %s. Attempting to fetch message." format (offset, checkpointTopic, partitionId))

          if (offset <= 0) {
            info("Got offset 0 (no messages in checkpoint topic) for topic %s and partition %s, so returning null. If you expected the checkpoint topic to have messages, you're probably going to lose data." format (checkpointTopic, partition))
            return null
          }

          val request = new FetchRequestBuilder()
            // Kafka returns 1 greater than the offset of the last message in
            // the topic, so subtract one to fetch the last message.
            .addFetch(checkpointTopic, partitionId, offset - 1, fetchSize)
            .maxWait(500)
            .minBytes(1)
            .clientId(clientId)
            .build
          val messageSet = consumer.fetch(request)
          if (messageSet.hasError) {
            warn("Got error code from broker for %s: %s" format (checkpointTopic, messageSet.errorCode(checkpointTopic, partitionId)))
            val errorCode = messageSet.errorCode(checkpointTopic, partitionId)
            if (ErrorMapping.OffsetOutOfRangeCode.equals(errorCode)) {
              warn("Got an offset out of range exception while getting last checkpoint for topic %s and partition %s, so returning a null offset to the KafkaConsumer. Let it decide what to do based on its autooffset.reset setting." format (checkpointTopic, partitionId))
              return null
            }
            ErrorMapping.maybeThrowException(errorCode)
          }
          val messages = messageSet.messageSet(checkpointTopic, partitionId).toList

          if (messages.length != 1) {
            throw new KafkaCheckpointException("Something really unexpected happened. Got %s "
              + "messages back when fetching from checkpoint topic %s and partition %s. "
              + "Expected one message. It would be unsafe to go on without the latest checkpoint, "
              + "so failing." format (messages.length, checkpointTopic, partition))
          }

          // Some back bending to go from message to checkpoint.
          val checkpoint = serde.fromBytes(Utils.readBytes(messages(0).message.payload))
          loop.done
          checkpoint
        } finally {
          consumer.close
        }
      },

      (exception, loop) => {
        exception match {
          case e: KafkaCheckpointException => throw e
          case e: Exception =>
            warn("While trying to read last checkpoint for topic %s and partition %s: %s. Retrying." format (checkpointTopic, partition, e))
            debug(e)
        }
      }
    ).getOrElse(throw new SamzaException("Failed to get checkpoint for partition %s" format partition.getPartitionId))

    info("Got checkpoint state for partition %s: %s" format (partition.getPartitionId, checkpoint))
    checkpoint
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

  def stop = {
    if(producer != null) {
      producer.close
    }
  }

  private def createTopic {
    info("Attempting to create checkpoint topic %s with %s partitions." format (checkpointTopic, totalPartitions))
    retryBackoff.run(
      loop => {
        val zkClient = connectZk()
        try {
          AdminUtils.createTopic(
            zkClient,
            checkpointTopic,
            totalPartitions,
            replicationFactor)
        } finally {
          zkClient.close
        }

        info("Created checkpoint topic %s." format checkpointTopic)
        loop.done
      },

      (exception, loop) => {
        exception match {
          case e: TopicExistsException =>
            info("Checkpoint topic %s already exists." format checkpointTopic)
            loop.done
          case e: Exception =>
            warn("Failed to create topic %s: %s. Retrying." format (checkpointTopic, e))
            debug(e)
        }
      }
    )
  }

  private def validateTopic {
    info("Validating checkpoint topic %s." format checkpointTopic)
    retryBackoff.run(
      loop => {
        val topicMetadataMap = TopicMetadataCache.getTopicMetadata(Set(checkpointTopic), systemName, metadataStore.getTopicInfo)
        val topicMetadata = topicMetadataMap(checkpointTopic)
        ErrorMapping.maybeThrowException(topicMetadata.errorCode)

        val partitionCount = topicMetadata.partitionsMetadata.length
        if (partitionCount != totalPartitions) {
          throw new KafkaCheckpointException("Checkpoint topic validation failed for topic %s because partition count %s did not match expected partition count %s." format (checkpointTopic, topicMetadata.partitionsMetadata.length, totalPartitions))
        }

        info("Successfully validated checkpoint topic %s." format checkpointTopic)
        loop.done
      },

      (exception, loop) => {
        exception match {
          case e: KafkaCheckpointException => throw e
          case e: Exception =>
            warn("While trying to validate topic %s: %s. Retrying." format (checkpointTopic, e))
            debug(e)
        }
      }
    )
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
