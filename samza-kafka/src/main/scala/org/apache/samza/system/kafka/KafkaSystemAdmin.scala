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

import org.apache.samza.Partition
import org.apache.samza.SamzaException
import org.apache.samza.system.SystemAdmin
import org.apache.samza.system.SystemStreamMetadata
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.ClientUtilTopicMetadataStore
import kafka.api._
import kafka.consumer.SimpleConsumer
import kafka.utils.Utils
import kafka.client.ClientUtils
import kafka.common.TopicAndPartition
import kafka.common.ErrorMapping
import kafka.cluster.Broker
import grizzled.slf4j.Logging
import java.util.UUID
import scala.collection.JavaConversions._
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata

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
   * The timeout to use for the simple consumer when fetching metadata from
   * Kafka. Equivalent to Kafka's socket.timeout.ms configuration.
   */
  timeout: Int = Int.MaxValue,

  /**
   * The buffer size to use for the simple consumer when fetching metadata
   * from Kafka. Equivalent to Kafka's socket.receive.buffer.bytes
   * configuration.
   */
  bufferSize: Int = 1024000,

  /**
   * The client ID to use for the simple consumer when fetching metadata from
   * Kafka. Equivalent to Kafka's client.id configuration.
   */
  clientId: String = UUID.randomUUID.toString) extends SystemAdmin with Logging {

  import KafkaSystemAdmin._

  /**
   * Given a set of stream names (topics), fetch metadata from Kafka for each
   * stream, and return a map from stream name to SystemStreamMetadata for
   * each stream. This method will return null for oldest and newest offsets
   * if a given SystemStreamPartition is empty. This method will block and
   * retry indefinitely until it gets a successful response from Kafka.
   */
  def getSystemStreamMetadata(streams: java.util.Set[String]) = {
    var partitions = Map[String, Set[Partition]]()
    var oldestOffsets = Map[SystemStreamPartition, String]()
    var newestOffsets = Map[SystemStreamPartition, String]()
    var upcomingOffsets = Map[SystemStreamPartition, String]()
    var done = false
    var consumer: SimpleConsumer = null

    debug("Fetching offsets for: %s" format streams)

    while (!done) {
      try {
        val metadata = TopicMetadataCache.getTopicMetadata(
          streams.toSet,
          systemName,
          getTopicMetadata)

        debug("Got metadata for streams: %s" format metadata)

        val brokersToTopicPartitions = getTopicsAndPartitionsByBroker(metadata)

        // Get oldest, newest, and upcoming offsets for each topic and partition.
        for ((broker, topicsAndPartitions) <- brokersToTopicPartitions) {
          debug("Fetching offsets for %s:%s: %s" format (broker.host, broker.port, topicsAndPartitions))

          consumer = new SimpleConsumer(broker.host, broker.port, timeout, bufferSize, clientId)
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
                debug("Stripping oldest/newest offsets for %s because the topic appears empty." format topicAndPartition)
                oldestOffsets -= topicAndPartition
                newestOffsets -= topicAndPartition
              }
          }

          debug("Shutting down consumer for %s:%s." format (broker.host, broker.port))

          consumer.close
        }

        done = true
      } catch {
        case e: InterruptedException =>
          info("Interrupted while fetching last offsets, so forwarding.")
          if (consumer != null) {
            consumer.close
          }
          throw e
        case e: Exception =>
          // Retry.
          warn("Unable to fetch last offsets for streams due to: %s, %s. Retrying. Turn on debugging to get a full stack trace." format (e.getMessage, streams))
          debug(e)
      }
    }

    assembleMetadata(oldestOffsets, newestOffsets, upcomingOffsets)
  }

  /**
   * Helper method to use topic metadata cache when fetching metadata, so we
   * don't hammer Kafka more than we need to.
   */
  private def getTopicMetadata(topics: Set[String]) = {
    new ClientUtilTopicMetadataStore(brokerListString, clientId)
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
      .flatMap(topicMetadata => topicMetadata
        .partitionsMetadata
        // Convert Seq[PartitionMetadata] to Seq[(Broker, TopicAndPartition)]
        .map(partitionMetadata => {
          ErrorMapping.maybeThrowException(partitionMetadata.errorCode)
          val topicAndPartition = new TopicAndPartition(topicMetadata.topic, partitionMetadata.partitionId)
          val leader = partitionMetadata
            .leader
            .getOrElse(throw new SamzaException("Need leaders for all partitions when fetching offsets. No leader available for TopicAndPartition: %s" format topicAndPartition))
          (leader, topicAndPartition)
        }))
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
        ErrorMapping.maybeThrowException(partitionErrorAndOffset.error)
        partitionErrorAndOffset.offsets.head
      })

    for ((topicAndPartition, offset) <- brokerOffsets) {
      offsets += new SystemStreamPartition(systemName, topicAndPartition.topic, new Partition(topicAndPartition.partition)) -> offset.toString
    }

    debug("Got offsets for %s using earliest/latest value of %s: %s" format (topicsAndPartitions, earliestOrLatest, offsets))

    offsets
  }
}
