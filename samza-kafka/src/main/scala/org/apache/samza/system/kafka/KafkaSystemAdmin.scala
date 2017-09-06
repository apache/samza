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

import java.util
import java.util.{Properties, UUID}

import kafka.admin.AdminUtils
import kafka.api._
import kafka.common.TopicAndPartition
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import kafka.utils.ZkUtils
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system._
import org.apache.samza.util.{ClientUtilTopicMetadataStore, ExponentialSleepStrategy, KafkaUtil, Logging}
import org.apache.samza.{Partition, SamzaException}

import scala.collection.JavaConverters._


object KafkaSystemAdmin extends Logging {

  val CLEAR_STREAM_RETRIES = 3

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
          val streamMetadata = new SystemStreamMetadata(streamName, streamPartitionMetadata.asJava)
          (streamName, streamMetadata)
      }
      .toMap

    // This is typically printed downstream and it can be spammy, so debug level here.
    debug("Got metadata: %s" format allMetadata)

    allMetadata
  }
}

/**
 * A helper class that is used to construct the changelog stream specific information
 *
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
   * A method that returns a ZkUtils for the Kafka system. This is invoked
   * when the system admin is attempting to create a coordinator stream.
   */
  connectZk: () => ZkUtils,

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
  topicMetaInformation: Map[String, ChangelogInfo] = Map[String, ChangelogInfo]()) extends ExtendedSystemAdmin with Logging {

  import KafkaSystemAdmin._

  override def getSystemStreamPartitionCounts(streams: util.Set[String], cacheTTL: Long): util.Map[String, SystemStreamMetadata] = {
    getSystemStreamPartitionCounts(streams, new ExponentialSleepStrategy(initialDelayMs = 500), cacheTTL)
  }

  def getSystemStreamPartitionCounts(streams: util.Set[String], retryBackoff: ExponentialSleepStrategy, cacheTTL: Long = Long.MaxValue): util.Map[String, SystemStreamMetadata] = {
    debug("Fetching system stream partition count for: %s" format streams)
    var metadataTTL = cacheTTL
    retryBackoff.run(
      loop => {
        val metadata = TopicMetadataCache.getTopicMetadata(
          streams.asScala.toSet,
          systemName,
          getTopicMetadata,
          metadataTTL)
        val result = metadata.map {
          case (topic, topicMetadata) => {
            KafkaUtil.maybeThrowException(topicMetadata.errorCode)
            val partitionsMap = topicMetadata.partitionsMetadata.map {
              pm =>
                new Partition(pm.partitionId) -> new SystemStreamPartitionMetadata("", "", "")
            }.toMap[Partition, SystemStreamPartitionMetadata]
            (topic -> new SystemStreamMetadata(topic, partitionsMap.asJava))
          }
        }
        loop.done
        result.asJava
      },

      (exception, loop) => {
        warn("Unable to fetch last offsets for streams %s due to %s. Retrying." format (streams, exception))
        debug("Exception detail:", exception)
        if (metadataTTL == Long.MaxValue) {
          metadataTTL = 5000 // Revert to the default cache expiration
        }
      }
    ).getOrElse(throw new SamzaException("Failed to get system stream metadata"))
  }

  /**
   * Returns the offset for the message after the specified offset for each
   * SystemStreamPartition that was passed in.
   */

  override def getOffsetsAfter(offsets: java.util.Map[SystemStreamPartition, String]) = {
    // This is safe to do with Kafka, even if a topic is key-deduped. If the
    // offset doesn't exist on a compacted topic, Kafka will return the first
    // message AFTER the offset that was specified in the fetch request.
    offsets.asScala.mapValues(offset => (offset.toLong + 1).toString).asJava
  }

  override def getSystemStreamMetadata(streams: java.util.Set[String]) =
    getSystemStreamMetadata(streams, new ExponentialSleepStrategy(initialDelayMs = 500)).asJava

  /**
   * Given a set of stream names (topics), fetch metadata from Kafka for each
   * stream, and return a map from stream name to SystemStreamMetadata for
   * each stream. This method will return null for oldest and newest offsets
   * if a given SystemStreamPartition is empty. This method will block and
   * retry indefinitely until it gets a successful response from Kafka.
   */
  def getSystemStreamMetadata(streams: java.util.Set[String], retryBackoff: ExponentialSleepStrategy) = {
    debug("Fetching system stream metadata for: %s" format streams)
    var metadataTTL = Long.MaxValue // Trust the cache until we get an exception
    retryBackoff.run(
      loop => {
        val metadata = TopicMetadataCache.getTopicMetadata(
          streams.asScala.toSet,
          systemName,
          getTopicMetadata,
          metadataTTL)

        debug("Got metadata for streams: %s" format metadata)

        val brokersToTopicPartitions = getTopicsAndPartitionsByBroker(metadata)
        var oldestOffsets = Map[SystemStreamPartition, String]()
        var newestOffsets = Map[SystemStreamPartition, String]()
        var upcomingOffsets = Map[SystemStreamPartition, String]()

        // Get oldest, newest, and upcoming offsets for each topic and partition.
        for ((broker, topicsAndPartitions) <- brokersToTopicPartitions) {
          debug("Fetching offsets for %s:%s: %s" format (broker.host, broker.port, topicsAndPartitions))

          val consumer = new SimpleConsumer(broker.host, broker.port, timeout, bufferSize, clientId)
          try {
            upcomingOffsets ++= getOffsets(consumer, topicsAndPartitions, OffsetRequest.LatestTime)
            oldestOffsets ++= getOffsets(consumer, topicsAndPartitions, OffsetRequest.EarliestTime)

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
                  oldestOffsets += (topicAndPartition -> "0")
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
        metadataTTL = 5000 // Revert to the default cache expiration
      }).getOrElse(throw new SamzaException("Failed to get system stream metadata"))
  }

  /**
   * Returns the newest offset for the specified SSP.
   * This method is fast and targeted. It minimizes the number of kafka requests.
   * It does not retry indefinitely if there is any failure.
   * It returns null if the topic is empty. To get the offsets for *all*
   * partitions, it would be more efficient to call getSystemStreamMetadata
   */
  override def getNewestOffset(ssp: SystemStreamPartition, maxRetries: Integer) = {
    debug("Fetching newest offset for: %s" format ssp)
    var offset: String = null
    var metadataTTL = Long.MaxValue // Trust the cache until we get an exception
    var retries = maxRetries
    new ExponentialSleepStrategy().run(
      loop => {
        val metadata = TopicMetadataCache.getTopicMetadata(
          Set(ssp.getStream),
          systemName,
          getTopicMetadata,
          metadataTTL)
        debug("Got metadata for streams: %s" format metadata)

        val brokersToTopicPartitions = getTopicsAndPartitionsByBroker(metadata)
        val topicAndPartition = new TopicAndPartition(ssp.getStream, ssp.getPartition.getPartitionId)
        val broker = brokersToTopicPartitions.filter((e) => e._2.contains(topicAndPartition)).head._1

        // Get oldest, newest, and upcoming offsets for each topic and partition.
        debug("Fetching offset for %s:%s: %s" format (broker.host, broker.port, topicAndPartition))
        val consumer = new SimpleConsumer(broker.host, broker.port, timeout, bufferSize, clientId)
        try {
          offset = getOffsets(consumer, Set(topicAndPartition), OffsetRequest.LatestTime).head._2

          // Kafka's "latest" offset is always last message in stream's offset +
          // 1, so get newest message in stream by subtracting one. this is safe
          // even for key-deduplicated streams, since the last message will
          // never be deduplicated.
          if (offset.toLong <= 0) {
            debug("Stripping newest offsets for %s because the topic appears empty." format topicAndPartition)
            offset = null
          } else {
            offset = (offset.toLong - 1).toString
          }
        } finally {
          consumer.close
        }

        debug("Got offset %s for %s." format(offset, ssp))
        loop.done
      },

      (exception, loop) => {
        if (retries > 0) {
          warn("Exception while trying to get offset for %s: %s. Retrying." format(ssp, exception))
          metadataTTL = 0L // Force metadata refresh
          retries -= 1
        } else {
          warn("Exception while trying to get offset for %s" format(ssp), exception)
          loop.done
          throw exception
        }
      })

     offset
  }

  /**
   * Helper method to use topic metadata cache when fetching metadata, so we
   * don't hammer Kafka more than we need to.
   */
  def getTopicMetadata(topics: Set[String]) = {
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

  /**
   * @inheritdoc
   */
  override def createStream(spec: StreamSpec): Boolean = {
    val kSpec = toKafkaSpec(spec)
    var streamCreated = false

    new ExponentialSleepStrategy(initialDelayMs = 500).run(
      loop => {
        val zkClient = connectZk()
        try {
          AdminUtils.createTopic(
            zkClient,
            kSpec.getPhysicalName,
            kSpec.getPartitionCount,
            kSpec.getReplicationFactor,
            kSpec.getProperties)
        } finally {
          zkClient.close
        }

        streamCreated = true
        loop.done
      },

      (exception, loop) => {
        exception match {
          case e: TopicExistsException =>
            streamCreated = false
            loop.done
          case e: Exception =>
            warn("Failed to create topic %s: %s. Retrying." format (spec.getPhysicalName, e))
            debug("Exception detail:", e)
        }
      })

    streamCreated
  }

  /**
   * Converts a StreamSpec into a KafakStreamSpec. Special handling for coordinator and changelog stream.
   * @param spec a StreamSpec object
   * @return KafkaStreamSpec object
   */
  def toKafkaSpec(spec: StreamSpec): KafkaStreamSpec = {
    spec.getId match {
      case StreamSpec.CHANGELOG_STREAM_ID =>
        val topicName = spec.getPhysicalName
        val topicMeta = topicMetaInformation.getOrElse(topicName, throw new KafkaChangelogException("Unable to find topic information for topic " + topicName))
        new KafkaStreamSpec(StreamSpec.CHANGELOG_STREAM_ID, topicName, systemName, spec.getPartitionCount, topicMeta.replicationFactor, topicMeta.kafkaProps)

      case StreamSpec.COORDINATOR_STREAM_ID =>
        new KafkaStreamSpec(StreamSpec.COORDINATOR_STREAM_ID, spec.getPhysicalName, systemName, 1, coordinatorStreamReplicationFactor, coordinatorStreamProperties)

      case _ =>
        KafkaStreamSpec.fromSpec(spec)
    }
  }

  /**
    * @inheritdoc
    *
    * Validates a stream in Kafka. Should not be called before createStream(),
    * since ClientUtils.fetchTopicMetadata(), used by different Kafka clients,
    * is not read-only and will auto-create a new topic.
    */
  override def validateStream(spec: StreamSpec): Unit = {
    val topicName = spec.getPhysicalName
    info("Validating topic %s." format topicName)

    val retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy
    var metadataTTL = Long.MaxValue // Trust the cache until we get an exception
    retryBackoff.run(
      loop => {
        val metadataStore = new ClientUtilTopicMetadataStore(brokerListString, clientId, timeout)
        val topicMetadataMap = TopicMetadataCache.getTopicMetadata(Set(topicName), systemName, metadataStore.getTopicInfo, metadataTTL)
        val topicMetadata = topicMetadataMap(topicName)
        KafkaUtil.maybeThrowException(topicMetadata.errorCode)

        val partitionCount = topicMetadata.partitionsMetadata.length
        if (partitionCount != spec.getPartitionCount) {
          throw new StreamValidationException("Topic validation failed for topic %s because partition count %s did not match expected partition count of %d" format (topicName, topicMetadata.partitionsMetadata.length, spec.getPartitionCount))
        }

        info("Successfully validated topic %s." format topicName)
        loop.done
      },

      (exception, loop) => {
        exception match {
          case e: StreamValidationException => throw e
          case e: Exception =>
            warn("While trying to validate topic %s: %s. Retrying." format (topicName, e))
            debug("Exception detail:", e)
            metadataTTL = 5000L // Revert to the default value
        }
      })
  }

  /**
   * @inheritdoc
   *
   * Delete a stream in Kafka. Deleting topics works only when the broker is configured with "delete.topic.enable=true".
   * Otherwise it's a no-op.
   */
  override def clearStream(spec: StreamSpec) = {
    val kSpec = KafkaStreamSpec.fromSpec(spec)
    var retries = CLEAR_STREAM_RETRIES
    new ExponentialSleepStrategy().run(
      loop => {
        val zkClient = connectZk()
        try {
          AdminUtils.deleteTopic(
            zkClient,
            kSpec.getPhysicalName)
        } finally {
          zkClient.close
        }

        loop.done
      },

      (exception, loop) => {
        if (retries > 0) {
          warn("Exception while trying to delete topic %s: %s. Retrying." format (spec.getPhysicalName, exception))
          retries -= 1
        } else {
          warn("Fail to delete topic %s: %s" format (spec.getPhysicalName, exception))
          loop.done
          throw exception
        }
      })
  }

  /**
    * Exception to be thrown when the change log stream creation or validation has failed
    */
  class KafkaChangelogException(s: String, t: Throwable) extends SamzaException(s, t) {
    def this(s: String) = this(s, null)
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
