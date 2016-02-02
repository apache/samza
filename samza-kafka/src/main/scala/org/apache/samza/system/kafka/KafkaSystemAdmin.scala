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

package org.apache.samza.system.kafka

import org.I0Itec.zkclient.ZkClient
import org.apache.samza.Partition
import org.apache.samza.SamzaException
import org.apache.samza.system.SystemAdmin
import org.apache.samza.system.SystemStreamMetadata
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.{ ClientUtilTopicMetadataStore, ExponentialSleepStrategy, Logging }
import kafka.api._
import kafka.consumer.SimpleConsumer
import kafka.common.{ TopicExistsException, TopicAndPartition }
import java.util.{ Properties, UUID }
import scala.collection.JavaConversions._
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import kafka.consumer.ConsumerConfig
import kafka.admin.AdminUtils
import org.apache.samza.util.KafkaUtil

object KafkaSystemAdmin extends Logging {
  /**
   * A helper method that takes oldest, newest, and upcoming offsets for each
   * system stream partition, and creates a single map from stream name to
   * SystemStreamMetadata.
   */
  def assembleMetadata(oldestOffsets: Map[SystemStreamPartition, String], newestOffsets: Map[SystemStreamPartition, String], upcomingOffsets: Map[SystemStreamPartition, String]): Map[String, SystemStreamMetadata] = {
    val allMetadata = (oldestOffsets.keySet ++ newestOffsets.keySet ++ upcomingOffsets.keySet)
      .groupBy(_.getStream)
      .map {
        case (streamName, systemStreamPartitions) =>
          val streamPartitionMetadata = systemStreamPartitions
            .map(systemStreamPartition => {
              val partitionMetadata = new SystemStreamPartitionMetadata(
                // If the topic/partition is empty then oldest and newest will 
                // be stripped of their offsets, so default to null.
                oldestOffsets.getOrElse(systemStreamPartition, null),
                newestOffsets.getOrElse(systemStreamPartition, null),
                upcomingOffsets(systemStreamPartition))
              (systemStreamPartition.getPartition, partitionMetadata)
            })
            .toMap
          val streamMetadata = new SystemStreamMetadata(streamName, streamPartitionMetadata)
          (streamName, streamMetadata)
      }
      .toMap

    info("Got metadata: %s" format allMetadata)

    allMetadata
  }
}

/**
 * A helper class that is used to construct the changelog stream specific information
 * @param replicationFactor The number of replicas for the changelog stream
 * @param kafkaProps The kafka specific properties that need to be used for changelog stream creation
 */
case class ChangelogInfo(var replicationFactor: Int, var kafkaProps: Properties)

/**
 * A Kafka-based implementation of SystemAdmin.
 */
class KafkaSystemAdmin(
  /**
   * The system name to use when creating SystemStreamPartitions to return in
   * the getSystemStreamMetadata responser.
   */
  systemName: String,

  // TODO whenever Kafka decides to make the Set[Broker] class public, let's switch to Set[Broker] here.
  /**
   * List of brokers that are part of the Kafka system that we wish to
   * interact with. The format is host1:port1,host2:port2.
   */
  brokerListString: String,

  /**
   * A method that returns a ZkClient for the Kafka system. This is invoked
   * when the system admin is attempting to create a coordinator stream.
   */
  connectZk: () => ZkClient,

  /**
   * Custom properties to use when the system admin tries to create a new
   * coordinator stream.
   */
  coordinatorStreamProperties: Properties = new Properties,

  /**
   * The replication factor to use when the system admin creates a new
   * coordinator stream.
   */
  coordinatorStreamReplicationFactor: Int = 1,

  /**
   * The timeout to use for the simple consumer when fetching metadata from
   * Kafka. Equivalent to Kafka's socket.timeout.ms configuration.
   */
  timeout: Int = Int.MaxValue,

  /**
   * The buffer size to use for the simple consumer when fetching metadata
   * from Kafka. Equivalent to Kafka's socket.receive.buffer.bytes
   * configuration.
   */
  bufferSize: Int = ConsumerConfig.SocketBufferSize,

  /**
   * The client ID to use for the simple consumer when fetching metadata from
   * Kafka. Equivalent to Kafka's client.id configuration.
   */
  clientId: String = UUID.randomUUID.toString,

  /**
   * Replication factor for the Changelog topic in kafka
   * Kafka properties to be used during the Changelog topic creation
   */
  topicMetaInformation: Map[String, ChangelogInfo] = Map[String, ChangelogInfo]()) extends SystemAdmin with Logging {

  import KafkaSystemAdmin._

  /**
   * Returns the offset for the message after the specified offset for each
   * SystemStreamPartition that was passed in.
   */
  def getOffsetsAfter(offsets: java.util.Map[SystemStreamPartition, String]) = {
    // This is safe to do with Kafka, even if a topic is key-deduped. If the 
    // offset doesn't exist on a compacted topic, Kafka will return the first 
    // message AFTER the offset that was specified in the fetch request.
    offsets.mapValues(offset => (offset.toLong + 1).toString)
  }

  def getSystemStreamMetadata(streams: java.util.Set[String]) =
    getSystemStreamMetadata(streams, new ExponentialSleepStrategy(initialDelayMs = 500))

  /**
   * Given a set of stream names (topics), fetch metadata from Kafka for each
   * stream, and return a map from stream name to SystemStreamMetadata for
   * each stream. This method will return null for oldest and newest offsets
   * if a given SystemStreamPartition is empty. This method will block and
   * retry indefinitely until it gets a successful response from Kafka.
   */
  def getSystemStreamMetadata(streams: java.util.Set[String], retryBackoff: ExponentialSleepStrategy) = {
    debug("Fetching system stream metadata for: %s" format streams)
    retryBackoff.run(
      loop => {
        val metadata = TopicMetadataCache.getTopicMetadata(
          streams.toSet,
          systemName,
          getTopicMetadata)

        debug("Got metadata for streams: %s" format metadata)

        val brokersToTopicPartitions = getTopicsAndPartitionsByBroker(metadata)
        var partitions = Map[String, Set[Partition]]()
        var oldestOffsets = Map[SystemStreamPartition, String]()
        var newestOffsets = Map[SystemStreamPartition, String]()
        var upcomingOffsets = Map[SystemStreamPartition, String]()

        // Get oldest, newest, and upcoming offsets for each topic and partition.
        for ((broker, topicsAndPartitions) <- brokersToTopicPartitions) {
          debug("Fetching offsets for %s:%s: %s" format (broker.host, broker.port, topicsAndPartitions))

          val consumer = new SimpleConsumer(broker.host, broker.port, timeout, bufferSize, clientId)
          try {
            oldestOffsets ++= getOffsets(consumer, topicsAndPartitions, OffsetRequest.EarliestTime)
            upcomingOffsets ++= getOffsets(consumer, topicsAndPartitions, OffsetRequest.LatestTime)
            // Kafka's "latest" offset is always last message in stream's offset +
            // 1, so get newest message in stream by subtracting one. this is safe
            // even for key-deduplicated streams, since the last message will
            // never be deduplicated.
            newestOffsets = upcomingOffsets.mapValues(offset => (offset.toLong - 1).toString)
            // Keep only oldest/newest offsets where there is a message. Should
            // return null offsets for empty streams.
            upcomingOffsets.foreach {
              case (topicAndPartition, offset) =>
                if (offset.toLong <= 0) {
                  debug("Stripping newest offsets for %s because the topic appears empty." format topicAndPartition)
                  newestOffsets -= topicAndPartition
                  debug("Setting oldest offset to 0 to consume from beginning")
                  oldestOffsets.get(topicAndPartition) match {
                    case Some(s) =>
                      oldestOffsets.updated(topicAndPartition, "0")
                    case None =>
                      oldestOffsets.put(topicAndPartition, "0")
                  }
                }
            }
          } finally {
            consumer.close
          }
        }

        val result = assembleMetadata(oldestOffsets, newestOffsets, upcomingOffsets)
        loop.done
        result
      },

      (exception, loop) => {
        warn("Unable to fetch last offsets for streams %s due to %s. Retrying." format (streams, exception))
        debug("Exception detail:", exception)
      }).getOrElse(throw new SamzaException("Failed to get system stream metadata"))
  }

  def createCoordinatorStream(streamName: String) {
    info("Attempting to create coordinator stream %s." format streamName)
    new ExponentialSleepStrategy(initialDelayMs = 500).run(
      loop => {
        val zkClient = connectZk()
        try {
          AdminUtils.createTopic(
            zkClient,
            streamName,
            1, // Always one partition for coordinator stream.
            coordinatorStreamReplicationFactor,
            coordinatorStreamProperties)
        } finally {
          zkClient.close
        }

        info("Created coordinator stream %s." format streamName)
        loop.done
      },

      (exception, loop) => {
        exception match {
          case e: TopicExistsException =>
            info("Coordinator stream %s already exists." format streamName)
            loop.done
          case e: Exception =>
            warn("Failed to create topic %s: %s. Retrying." format (streamName, e))
            debug("Exception detail:", e)
        }
      })
  }

  /**
   * Helper method to use topic metadata cache when fetching metadata, so we
   * don't hammer Kafka more than we need to.
   */
  protected def getTopicMetadata(topics: Set[String]) = {
    new ClientUtilTopicMetadataStore(brokerListString, clientId, timeout)
      .getTopicInfo(topics)
  }

  /**
   * Break topic metadata topic/partitions into per-broker map so that we can
   * execute only one offset request per broker.
   */
  private def getTopicsAndPartitionsByBroker(metadata: Map[String, TopicMetadata]) = {
    val brokersToTopicPartitions = metadata
      .values
      // Convert the topic metadata to a Seq[(Broker, TopicAndPartition)] 
      .flatMap(topicMetadata => {
        KafkaUtil.maybeThrowException(topicMetadata.errorCode)
        topicMetadata
          .partitionsMetadata
          // Convert Seq[PartitionMetadata] to Seq[(Broker, TopicAndPartition)]
          .map(partitionMetadata => {
            val topicAndPartition = new TopicAndPartition(topicMetadata.topic, partitionMetadata.partitionId)
            val leader = partitionMetadata
              .leader
              .getOrElse(throw new SamzaException("Need leaders for all partitions when fetching offsets. No leader available for TopicAndPartition: %s" format topicAndPartition))
            (leader, topicAndPartition)
          })
      })

      // Convert to a Map[Broker, Seq[(Broker, TopicAndPartition)]]
      .groupBy(_._1)
      // Convert to a Map[Broker, Set[TopicAndPartition]]
      .mapValues(_.map(_._2).toSet)

    debug("Got topic partition data for brokers: %s" format brokersToTopicPartitions)

    brokersToTopicPartitions
  }

  /**
   * Use a SimpleConsumer to fetch either the earliest or latest offset from
   * Kafka for each topic/partition in the topicsAndPartitions set. It is
   * assumed that all topics/partitions supplied reside on the broker that the
   * consumer is connected to.
   */
  private def getOffsets(consumer: SimpleConsumer, topicsAndPartitions: Set[TopicAndPartition], earliestOrLatest: Long) = {
    debug("Getting offsets for %s using earliest/latest value of %s." format (topicsAndPartitions, earliestOrLatest))

    var offsets = Map[SystemStreamPartition, String]()
    val partitionOffsetInfo = topicsAndPartitions
      .map(topicAndPartition => (topicAndPartition, PartitionOffsetRequestInfo(earliestOrLatest, 1)))
      .toMap
    val brokerOffsets = consumer
      .getOffsetsBefore(new OffsetRequest(partitionOffsetInfo))
      .partitionErrorAndOffsets
      .mapValues(partitionErrorAndOffset => {
        KafkaUtil.maybeThrowException(partitionErrorAndOffset.error)
        partitionErrorAndOffset.offsets.head
      })

    for ((topicAndPartition, offset) <- brokerOffsets) {
      offsets += new SystemStreamPartition(systemName, topicAndPartition.topic, new Partition(topicAndPartition.partition)) -> offset.toString
    }

    debug("Got offsets for %s using earliest/latest value of %s: %s" format (topicsAndPartitions, earliestOrLatest, offsets))

    offsets
  }

  private def createTopicInKafka(topicName: String, numKafkaChangelogPartitions: Int) {
    val retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy
    info("Attempting to create change log topic %s." format topicName)
    info("Using partition count " + numKafkaChangelogPartitions + " for creating change log topic")
    val topicMetaInfo = topicMetaInformation.getOrElse(topicName, throw new KafkaChangelogException("Unable to find topic information for topic " + topicName))
    retryBackoff.run(
      loop => {
        val zkClient = connectZk()
        try {
          AdminUtils.createTopic(
            zkClient,
            topicName,
            numKafkaChangelogPartitions,
            topicMetaInfo.replicationFactor,
            topicMetaInfo.kafkaProps)
        } finally {
          zkClient.close
        }

        info("Created changelog topic %s." format topicName)
        loop.done
      },

      (exception, loop) => {
        exception match {
          case e: TopicExistsException =>
            info("Changelog topic %s already exists." format topicName)
            loop.done
          case e: Exception =>
            warn("Failed to create topic %s: %s. Retrying." format (topicName, e))
            debug("Exception detail:", e)
        }
      })
  }

  private def validateTopicInKafka(topicName: String, numKafkaChangelogPartitions: Int) {
    val retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy
    info("Validating changelog topic %s." format topicName)
    retryBackoff.run(
      loop => {
        val metadataStore = new ClientUtilTopicMetadataStore(brokerListString, clientId, timeout)
        val topicMetadataMap = TopicMetadataCache.getTopicMetadata(Set(topicName), systemName, metadataStore.getTopicInfo)
        val topicMetadata = topicMetadataMap(topicName)
        KafkaUtil.maybeThrowException(topicMetadata.errorCode)

        val partitionCount = topicMetadata.partitionsMetadata.length
        if (partitionCount < numKafkaChangelogPartitions) {
          throw new KafkaChangelogException("Changelog topic validation failed for topic %s because partition count %s did not match expected partition count of %d" format (topicName, topicMetadata.partitionsMetadata.length, numKafkaChangelogPartitions))
        }

        info("Successfully validated changelog topic %s." format topicName)
        loop.done
      },

      (exception, loop) => {
        exception match {
          case e: KafkaChangelogException => throw e
          case e: Exception =>
            warn("While trying to validate topic %s: %s. Retrying." format (topicName, e))
            debug("Exception detail:", e)
        }
      })
  }

  /**
   * Exception to be thrown when the change log stream creation or validation has failed
   */
  class KafkaChangelogException(s: String, t: Throwable) extends SamzaException(s, t) {
    def this(s: String) = this(s, null)
  }

  override def createChangelogStream(topicName: String, numKafkaChangelogPartitions: Int) = {
    createTopicInKafka(topicName, numKafkaChangelogPartitions)
    validateChangelogStream(topicName, numKafkaChangelogPartitions)
  }

  /**
   * Validates change log stream in Kafka. Should not be called before createChangelogStream(),
   * since ClientUtils.fetchTopicMetadata(), used by different Kafka clients, is not read-only and
   * will auto-create a new topic.
   */
  override def validateChangelogStream(topicName: String, numKafkaChangelogPartitions: Int) = {
    validateTopicInKafka(topicName, numKafkaChangelogPartitions)
  }

  /**
   * Compare the two offsets. Returns x where x < 0 if offset1 < offset2;
   * x == 0 if offset1 == offset2; x > 0 if offset1 > offset2.
   *
   * Currently it's used in the context of the broadcast streams to detect
   * the mismatch between two streams when consuming the broadcast streams.
   */
  override def offsetComparator(offset1: String, offset2: String) = {
    offset1.toLong compare offset2.toLong
  }
}
